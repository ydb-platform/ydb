# -*- coding: utf-8 -*-
import hashlib
import re
import time

import pytest
import requests

from ydb.tests.functional.security.lib.security_test_helpers import (
    _test_endpoints,
    tablet_devui_expected_on_app,
    tablet_devui_new_action_paths,
    tablet_devui_sid_matrix,
)
from ydb.tests.library.clients.kikimr_http_client import DEFAULT_HIVE_ID
from ydb.tests.oss.ydb_sdk_import import ydb

# MakeBSControllerID(): (1 << 56) | 0x1001
BSC_TABLET_ID = 72057594037932033


def _is_valid_tablet_id(tablet_id):
    return tablet_id not in (None, 0)


def _tablet_id_from_partition_stats(pool, table_path):
    database = table_path.rsplit('/', 1)[0]
    partition_stats_path = f'{database}/.sys/partition_stats'

    def fetch_tablet_id(session):
        query = f"""
            SELECT TabletId
            FROM `{partition_stats_path}`
            WHERE Path = "{table_path}" AND TabletId > 0
            LIMIT 1;
        """
        result_sets = session.transaction().execute(query, commit_tx=True)
        if not result_sets or not result_sets[0].rows:
            return None

        row = result_sets[0].rows[0]
        if isinstance(row, dict):
            tablet_id = row.get('TabletId')
        else:
            tablet_id = getattr(row, 'TabletId', None)
        if _is_valid_tablet_id(tablet_id):
            return tablet_id
        return None

    return pool.retry_operation_sync(fetch_tablet_id)


def _tablet_id_from_viewer_describe(cluster, table_path):
    node = cluster.nodes[1]
    database = table_path.rsplit('/', 1)[0]
    response = requests.get(
        f'https://{node.host}:{node.mon_port}/viewer/json/describe',
        params={
            'database': database,
            'path': table_path,
            'partition_stats': 'true',
            'subs': '0',
            'enums': 'true',
        },
        headers={'Authorization': 'root@builtin'},
        verify=False,
        timeout=1,
    )
    response.raise_for_status()
    for partition in response.json()['PathDescription'].get('TablePartitions', []):
        tablet_id = partition.get('DatashardId') or partition.get('TabletId')
        if _is_valid_tablet_id(tablet_id):
            return tablet_id
    return None


def _schemeshard_tablet_id_from_viewer(cluster, database='/Root'):
    node = cluster.nodes[1]
    response = requests.get(
        f'https://{node.host}:{node.mon_port}/viewer/json/describe',
        params={
            'database': '/Root',
            'path': database,
            'enums': 'true',
        },
        headers={'Authorization': 'root@builtin'},
        verify=False,
        timeout=10,
    )
    response.raise_for_status()
    path_description = response.json()['PathDescription']
    processing_params = path_description.get('DomainDescription', {}).get('ProcessingParams', {})
    if 'SchemeShard' in processing_params:
        return int(processing_params['SchemeShard'])
    return int(path_description['Self']['SchemeshardId'])


@pytest.fixture(scope='module')
def ydb_cluster_with_enforce_user_token_and_schemeshard_tablet(ydb_cluster_with_enforce_user_token):
    cluster = ydb_cluster_with_enforce_user_token
    cluster.schemeshard_tablet_id = _schemeshard_tablet_id_from_viewer(cluster)
    yield cluster


@pytest.fixture(scope='module')
def ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet(
    ydb_cluster_with_enforce_user_token_and_tablet_devui_secure_path_flag,
):
    cluster = ydb_cluster_with_enforce_user_token_and_tablet_devui_secure_path_flag
    cluster.schemeshard_tablet_id = _schemeshard_tablet_id_from_viewer(cluster)
    yield cluster


@pytest.fixture(scope='module')
def ydb_cluster_with_enforce_user_token_and_datashard_tablet(ydb_cluster_with_enforce_user_token):
    cluster = ydb_cluster_with_enforce_user_token
    _prepare_datashard_tablet(cluster)
    yield cluster


@pytest.fixture(scope='module')
def ydb_cluster_with_enforce_user_token_secure_devui_flag_and_datashard_tablet(
    ydb_cluster_with_enforce_user_token_and_tablet_devui_secure_path_flag,
):
    cluster = ydb_cluster_with_enforce_user_token_and_tablet_devui_secure_path_flag
    _prepare_datashard_tablet(cluster)
    yield cluster


def _prepare_datashard_tablet(cluster, database='/Root/ds_mon_security'):
    cluster.create_database(
        database,
        storage_pool_units_count={'hdd': 1},
        token='root@builtin',
    )
    cluster.register_and_start_slots(database, count=1)
    cluster.wait_tenant_up(database, token='root@builtin')

    node = cluster.nodes[1]
    driver_config = ydb.DriverConfig(
        endpoint=f'{node.host}:{node.port}',
        database=database,
        credentials=ydb.AuthTokenCredentials('root@builtin'),
    )
    table_path = f'{database}/ds_mon_t'
    with ydb.Driver(driver_config) as driver:
        driver.wait(timeout=5)

        def create_table(session):
            session.create_table(
                table_path,
                ydb.TableDescription()
                .with_column(ydb.Column('id', ydb.OptionalType(ydb.PrimitiveType.Uint64)))
                .with_primary_key('id'),
            )
            # Force the first DataShard activity right after table creation.
            session.transaction().execute(
                f'UPSERT INTO `{table_path}` (id) VALUES (1);',
                commit_tx=True,
            )

        with ydb.SessionPool(driver) as pool:
            pool.retry_operation_sync(create_table)

            datashard_tablet_id = None
            poll_deadline = time.time() + 10
            poll_interval_seconds = 0.05
            while time.time() < poll_deadline:
                tid = _tablet_id_from_partition_stats(pool, table_path)
                if not _is_valid_tablet_id(tid):
                    tid = _tablet_id_from_viewer_describe(cluster, table_path)
                if _is_valid_tablet_id(tid):
                    datashard_tablet_id = tid
                    break
                time.sleep(poll_interval_seconds)
                poll_interval_seconds = min(0.5, poll_interval_seconds * 2)

    assert datashard_tablet_id, 'DataShard tablet id not available after CREATE TABLE and UPSERT'
    cluster.datashard_tablet_id = datashard_tablet_id


def _data_shard_devui_mon_paths_with_enforce(datashard_tablet_id, secure_path_mode):
    q = f'TabletID={datashard_tablet_id}'
    q_mutating_page = f'{q}&page=volatile-txs'
    q_mutating_action = f'{q}&action=key-access-sample'
    all_forbidden, monitoring_allowed_sids_ok, admin_allowed_sids_ok = tablet_devui_sid_matrix()
    expected_on_app = tablet_devui_expected_on_app(secure_path_mode, monitoring_allowed_sids_ok, all_forbidden)
    return {
        # New secure path for DataShard DevUI. Should be admin-only in both modes.
        f'/tablets/app/secure?{q}': admin_allowed_sids_ok,
        f'/tablets/app/secure?{q_mutating_page}': admin_allowed_sids_ok,
        f'/tablets/app/secure?{q_mutating_action}': admin_allowed_sids_ok,
        # Legacy path behavior depends on the feature flag:
        # - secure_path_mode=False: monitoring/root may access (legacy compatibility)
        # - secure_path_mode=True: denied for everyone, including root (force secure path usage)
        f'/tablets/app?{q}': expected_on_app,
        f'/tablets/app?{q_mutating_page}': expected_on_app,
        f'/tablets/app?{q_mutating_action}': expected_on_app,
        # Tablets summary page keeps monitoring-level access.
        f'/tablets?{q}': monitoring_allowed_sids_ok,
    }


def test_datashard_tablet_devui_mon_paths_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token_and_datashard_tablet,
):
    tid = ydb_cluster_with_enforce_user_token_and_datashard_tablet.datashard_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_and_datashard_tablet,
        _data_shard_devui_mon_paths_with_enforce(tid, secure_path_mode=False),
    )


def test_datashard_tablet_devui_mon_paths_with_enforce_user_token_and_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_datashard_tablet,
):
    tid = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_datashard_tablet.datashard_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_secure_devui_flag_and_datashard_tablet,
        _data_shard_devui_mon_paths_with_enforce(tid, secure_path_mode=True),
    )


@pytest.fixture(scope='module')
def ydb_cluster_with_enforce_user_token_and_hive_tablet(ydb_cluster_with_enforce_user_token):
    cluster = ydb_cluster_with_enforce_user_token
    cluster.hive_tablet_id = DEFAULT_HIVE_ID
    yield cluster


@pytest.fixture(scope='module')
def ydb_cluster_with_enforce_user_token_secure_devui_flag_and_hive_tablet(
    ydb_cluster_with_enforce_user_token_and_tablet_devui_secure_path_flag,
):
    cluster = ydb_cluster_with_enforce_user_token_and_tablet_devui_secure_path_flag
    cluster.hive_tablet_id = DEFAULT_HIVE_ID
    yield cluster


def _hive_endpoint_cases(endpoint_paths, token_statuses):
    return [
        (endpoint_path, token, expected_status)
        for endpoint_path in endpoint_paths
        for token, expected_status in token_statuses.items()
    ]


def _hive_token_desc(token):
    return token if token is not None else 'null'


def _hive_mon_base_url(cluster):
    node = cluster.nodes[1]
    return f'https://{node.host}:{node.mon_port}'


def _hive_get_status(cluster, endpoint_path, token=None):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    response = requests.get(
        f'{_hive_mon_base_url(cluster)}{endpoint_path}',
        headers=headers,
        verify=False,
    )
    return response.status_code


def _hive_post(cluster, endpoint_path, token=None):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    return requests.post(
        f'{_hive_mon_base_url(cluster)}{endpoint_path}',
        headers=headers,
        verify=False,
    )


def _hive_post_status(cluster, endpoint_path, token=None):
    return _hive_post(cluster, endpoint_path, token).status_code


def _hive_app_path(secure_path_mode):
    return '/tablets/app/secure' if secure_path_mode else '/tablets/app'


def _hive_request(cluster, secure_path_mode, query, method='GET'):
    url = f'{_hive_mon_base_url(cluster)}{_hive_app_path(secure_path_mode)}?TabletID={cluster.hive_tablet_id}&{query}'
    response = requests.request(method, url, headers={'Authorization': 'root@builtin'}, verify=False)
    response.raise_for_status()
    return response


def _hive_alive_node_id(cluster, secure_path_mode):
    nodes = _hive_request(cluster, secure_path_mode, 'page=MemStateNodes&format=json').json()['Nodes']
    alive = [node['Id'] for node in nodes if node.get('Alive')]
    assert alive, f'Hive reports no alive nodes: {nodes}'
    return alive[0]


def _hive_ensure_tenant(cluster):
    if getattr(cluster, 'hive_tenant_ready', False):
        return
    cluster.create_database(
        '/Root/hive_mon_security',
        storage_pool_units_count={'hdd': 1},
        token='root@builtin',
    )
    cluster.register_and_start_slots('/Root/hive_mon_security', count=1)
    cluster.wait_tenant_up('/Root/hive_mon_security', token='root@builtin')
    cluster.hive_tenant_ready = True


def _hive_managed_tablet_ids(cluster, secure_path_mode):
    _hive_ensure_tenant(cluster)
    # A tenant DataShard is owned by the tenant Hive, so the tablets are taken from this Hive.
    page = _hive_request(cluster, secure_path_mode, 'page=MemStateTablets&max=100').text
    return [int(tablet_id) for tablet_id in re.findall(r'tablets\?TabletID=(\d+)', page)]


def _hive_managed_tablet_id(cluster, secure_path_mode):
    tablet_ids = _hive_managed_tablet_ids(cluster, secure_path_mode)
    assert tablet_ids, 'Hive manages no tablets'
    return tablet_ids[0]


def _hive_destroy_key(tablet_id):
    # IsSafeOperation() hashes the concatenation of the tablet, owner and owner_idx cgi values.
    return hashlib.md5(str(tablet_id).encode()).hexdigest()


def _hive_pages(node_id, tablet_id, domain_ss, domain_path):
    return (
        ('', 200),
        ('page=LandingData', 200),
        ('page=MemStateNodes', 200),
        ('page=MemStateTablets', 200),
        ('page=MemStateDomains', 200),
        ('page=DbState', 200),
        ('page=Resources', 200),
        ('page=Groups', 200),
        ('page=Storage', 200),
        ('page=Settings', 200),
        ('page=Subactors', 200),
        ('page=OperationsLog&max=10', 200),
        ('page=ManualOperations', 200),
        ('page=ObjectStats', 200),
        ('page=QueryMigration', 200),
        (f'page=TabletInfo&tablet={tablet_id}', 200),
        (f'page=SetDown&node={node_id}&down=0', 200),
        (f'page=SetFreeze&node={node_id}&freeze=0', 200),
        (f'page=KickNode&node={node_id}', 200),
        (f'page=DrainNode&node={node_id}&wait=0', 200),
        (f'page=TabletAvailability&node={node_id}&resettype=Dummy', 200),
        ('page=Rebalance', 200),
        ('page=RebalanceFromScratch', 200),
        ('page=StorageRebalance', 200),
        ('page=ReassignTablet&tablet=all&wait=0', 200),
        (f'page=MoveTablet&tablet={tablet_id}&node={node_id}', 200),
        (f'page=StopTablet&tablet={tablet_id}', 200),
        (f'page=ResumeTablet&tablet={tablet_id}', 200),
        (f'page=UpdateResources&tablet={tablet_id}&cpu=1', 200),
        (f'page=StopDomain&ss={domain_ss}&path={domain_path}&stop=1', 200),
        (f'page=StopDomain&ss={domain_ss}&path={domain_path}&stop=0', 200),
        ('page=Settings&BootQueueUpdatePeriod=1000', 200),
        ('page=Subactors&stop=1', 200),
        ('page=InitMigration', 400),  # cannot migrate to the root hive
        (f'page=SetDomain&tablet={tablet_id}', 400),  # the tablet already belongs to this domain
        ('page=NewAction', 200),
    )


def _hive_expected(matrix, admin_status):
    return {token: (admin_status if status == 200 else status) for token, status in matrix.items()}


def _hive_assert_status(response, expected_status, endpoint_path, token):
    if expected_status is None:
        assert response.status_code not in (401, 403), (
            f'Expected POST {endpoint_path} with token={_hive_token_desc(token)} '
            f'to pass the access check, got {response.status_code}: {response.text[:300]}'
        )
    else:
        assert response.status_code == expected_status, (
            f'Expected POST {endpoint_path} with token={_hive_token_desc(token)} '
            f'to return {expected_status}, got {response.status_code}: {response.text[:300]}'
        )


def _hive_devui_cases(tablet_id, pages, secure_path_mode):
    q_base = f'TabletID={tablet_id}'
    all_forbidden, monitoring_allowed, admin_allowed = tablet_devui_sid_matrix()
    on_app = tablet_devui_expected_on_app(secure_path_mode, monitoring_allowed, all_forbidden)
    cases = []
    for query_suffix, admin_status in pages:
        q = q_base if not query_suffix else f'{q_base}&{query_suffix}'
        # The page handler only runs on the path the flag designates. On the other path the
        # request is either denied outright or not routed into the DevUI at all, in which case
        # the generic tablet page is rendered with 200 whatever the page parameter says.
        app_status = admin_status if not secure_path_mode else 200
        secure_status = admin_status if secure_path_mode else 200
        cases.extend(_hive_endpoint_cases([f'/tablets/app?{q}'], _hive_expected(on_app, app_status)))
        cases.extend(_hive_endpoint_cases([f'/tablets/app/secure?{q}'], _hive_expected(admin_allowed, secure_status)))
    # Tablets summary page is a different handler and keeps monitoring-level access.
    cases.extend(_hive_endpoint_cases([f'/tablets?{q_base}'], monitoring_allowed))
    return cases


def _hive_sweep(cluster, secure_path_mode):
    node_id = _hive_alive_node_id(cluster, secure_path_mode)
    tablet_id = _hive_managed_tablet_id(cluster, secure_path_mode)
    domain_ss = _schemeshard_tablet_id_from_viewer(cluster)
    pages = _hive_pages(node_id, tablet_id, domain_ss, 1)
    for endpoint_path, token, expected_status in _hive_devui_cases(
        cluster.hive_tablet_id, pages, secure_path_mode
    ):
        response = _hive_post(cluster, endpoint_path, token)
        _hive_assert_status(response, expected_status, endpoint_path, token)


def test_hive_tablet_devui_mon_paths_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token_and_hive_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_and_hive_tablet
    _hive_sweep(cluster, secure_path_mode=False)


def test_hive_tablet_devui_mon_paths_with_enforce_user_token_and_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_hive_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_hive_tablet
    _hive_sweep(cluster, secure_path_mode=True)


def test_hive_destroy_operations_with_secure_path_mode(
    ydb_cluster_with_secure_devui_flag_and_hive_destroy_operations,
):
    cluster = ydb_cluster_with_secure_devui_flag_and_hive_destroy_operations
    cluster.hive_tablet_id = DEFAULT_HIVE_ID
    _, _, admin_allowed = tablet_devui_sid_matrix()
    non_admins = {token: status for token, status in admin_allowed.items() if token != 'root@builtin'}

    tablet_id = _hive_managed_tablet_id(cluster, True)
    # Reset first, then delete the same tablet: both are exercised end to end.
    for page in ('ResetTablet', 'DeleteTablet'):
        q = f'TabletID={cluster.hive_tablet_id}&page={page}&tablet={tablet_id}&key={_hive_destroy_key(tablet_id)}'

        for token, expected_status in non_admins.items():
            for endpoint_path in (f'/tablets/app?{q}', f'/tablets/app/secure?{q}'):
                status = _hive_post_status(cluster, endpoint_path, token)
                assert status == expected_status, (
                    f'Expected POST {endpoint_path} with token={_hive_token_desc(token)} '
                    f'to return {expected_status}, got {status}'
                )

        status = _hive_post_status(cluster, f'/tablets/app?{q}', 'root@builtin')
        assert status == 403, f'Expected POST {page} on the legacy path to be denied, got {status}'

        response = _hive_post(cluster, f'/tablets/app/secure?{q}', 'root@builtin')
        assert response.status_code == 200, (
            f'Expected POST {page} as administrator to succeed, '
            f'got {response.status_code}: {response.text[:300]}'
        )


def test_hive_devui_links_stay_on_current_app_path(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_hive_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_hive_tablet
    tid = cluster.hive_tablet_id
    response = requests.get(
        f'{_hive_mon_base_url(cluster)}/tablets/app/secure?TabletID={tid}',
        headers={'Authorization': 'root@builtin'},
        verify=False,
    )

    assert response.status_code == 200, response.text
    assert f'location.href="?TabletID={tid}&page=MemStateNodes"' in response.text
    assert "'?TabletID=' + hiveId" in response.text
    assert 'hive.js' not in response.text
    assert f'href="app?TabletID={tid}' not in response.text
    assert f"href='app?TabletID={tid}" not in response.text


def _schemeshard_endpoint_cases(endpoint_paths, token_statuses):
    return [
        (endpoint_path, token, expected_status)
        for endpoint_path in endpoint_paths
        for token, expected_status in token_statuses.items()
    ]


def _schemeshard_public_page_access_cases(
    tablet_id,
    pages=(
        ('Main', ''),
        ('TxList', 'Page=TxList'),
        ('TxInfo', 'Page=TxInfo&TxId=0'),
        ('PathInfo', 'Page=PathInfo&OwnerPathId=0&LocalPathId=0'),
        ('ShardInfoByTabletId', 'Page=ShardInfoByTabletId&ShardID=0'),
        ('ShardInfoByShardIdx', 'Page=ShardInfoByShardIdx&OwnerShardIdx=0&LocalShardIdx=0'),
        ('BuildIndexInfo', 'Page=BuildIndexInfo&BuildIndexId=0'),
    ),
):
    q_base = f'TabletID={tablet_id}'
    _, monitoring_allowed, _ = tablet_devui_sid_matrix()
    cases = []
    for _, query_suffix in pages:
        q = q_base if not query_suffix else f'{q_base}&{query_suffix}'
        cases.extend(_schemeshard_endpoint_cases([f'/tablets/app?{q}'], monitoring_allowed))
    return cases


def _schemeshard_admin_page_access_cases(
    tablet_id,
    pages=(
        ('Admin', 'Page=Admin'),
        ('AdminRequest', 'Page=AdminRequest&UpdateAccessDatabaseRights=1&UpdateAccessDatabaseRightsDryRun=1'),
    ),
):
    q_base = f'TabletID={tablet_id}'
    all_forbidden, _, admin_allowed = tablet_devui_sid_matrix()
    cases = []
    for _, query_suffix in pages:
        q = f'{q_base}&{query_suffix}'
        cases.extend(_schemeshard_endpoint_cases([f'/tablets/app?{q}'], all_forbidden))
        cases.extend(_schemeshard_endpoint_cases([f'/tablets/app/secure?{q}'], admin_allowed))
    return cases


def _schemeshard_monitoring_devui_cases(tablet_id):
    q = f'TabletID={tablet_id}'
    _, monitoring_allowed, _ = tablet_devui_sid_matrix()
    return _schemeshard_endpoint_cases([f'/tablets/app?{q}', f'/tablets?{q}'], monitoring_allowed)


def _schemeshard_admin_devui_cases(tablet_id, secure_path_mode):
    q = f'TabletID={tablet_id}'
    all_forbidden, monitoring_allowed, admin_allowed = tablet_devui_sid_matrix()
    expected_on_app = tablet_devui_expected_on_app(secure_path_mode, monitoring_allowed, all_forbidden)
    admin_request = 'Page=AdminRequest&UpdateAccessDatabaseRights=1&UpdateAccessDatabaseRightsDryRun=1'
    return (
        _schemeshard_endpoint_cases([f'/tablets/app?{q}&Page=Admin'], expected_on_app)
        + _schemeshard_endpoint_cases([f'/tablets/app/secure?{q}&Page=Admin'], admin_allowed)
        + _schemeshard_endpoint_cases([f'/tablets/app?{q}&{admin_request}'], expected_on_app)
        + _schemeshard_endpoint_cases([f'/tablets/app/secure?{q}&{admin_request}'], admin_allowed)
    )


def _schemeshard_new_action_cases(tablet_id, query_suffix, secure_path_mode):
    expectations = tablet_devui_new_action_paths(tablet_id, query_suffix, secure_path_mode)
    cases = []
    for endpoint_path, token_statuses in expectations.items():
        cases.extend(_schemeshard_endpoint_cases([endpoint_path], token_statuses))
    return cases


def _schemeshard_post_action_paths(tablet_id, action, extra_params='', admin_secure_status=200, page=None):
    q = f'TabletID={tablet_id}'
    if page is not None:
        q = f'{q}&Page={page}'
    q = f'{q}&Action={action}'
    if extra_params:
        q = f'{q}&{extra_params}'
    return f'/tablets/app?{q}', f'/tablets/app/secure?{q}', admin_secure_status


def _schemeshard_post_access_cases(legacy_path, secure_path, admin_secure_status=200):
    all_forbidden, _, admin_allowed = tablet_devui_sid_matrix()
    expected_on_secure = dict(admin_allowed)
    expected_on_secure['root@builtin'] = admin_secure_status
    return _schemeshard_endpoint_cases([legacy_path], all_forbidden) + _schemeshard_endpoint_cases(
        [secure_path], expected_on_secure
    )


def _schemeshard_token_desc(token):
    return token if token is not None else 'null'


def _schemeshard_mon_base_url(cluster):
    node = cluster.nodes[1]
    return f'https://{node.host}:{node.mon_port}'


def _schemeshard_post_form_body(endpoint_path, post_data):
    if '?' not in endpoint_path:
        return post_data
    _, query = endpoint_path.rsplit('?', 1)
    if not query:
        return post_data
    if not post_data:
        return query
    return f'{query}&{post_data}'


def _schemeshard_get_response(cluster, endpoint_path, token=None):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    return requests.get(
        f'{_schemeshard_mon_base_url(cluster)}{endpoint_path}',
        headers=headers,
        verify=False,
    )


def _schemeshard_get_status(cluster, endpoint_path, token=None):
    return _schemeshard_get_response(cluster, endpoint_path, token).status_code


def _schemeshard_post_status(cluster, endpoint_path, token=None, post_data=''):
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    if token is not None:
        headers['Authorization'] = token
    response = requests.post(
        f'{_schemeshard_mon_base_url(cluster)}{endpoint_path}',
        headers=headers,
        data=_schemeshard_post_form_body(endpoint_path, post_data),
        verify=False,
        allow_redirects=False,
    )
    return response.status_code


def test_schemeshard_tablet_devui_mon_paths_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token_and_schemeshard_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_and_schemeshard_tablet
    tid = cluster.schemeshard_tablet_id
    cases = _schemeshard_monitoring_devui_cases(tid) + _schemeshard_admin_devui_cases(tid, secure_path_mode=False)

    for endpoint_path, token, expected_status in cases:
        status = _schemeshard_get_status(cluster, endpoint_path, token)
        assert status == expected_status, (
            f'Expected GET {endpoint_path} with token={_schemeshard_token_desc(token)} '
            f'to return {expected_status}, got {status}'
        )


def test_schemeshard_tablet_devui_mon_paths_with_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet
    tid = cluster.schemeshard_tablet_id
    cases = _schemeshard_monitoring_devui_cases(tid) + _schemeshard_admin_devui_cases(tid, secure_path_mode=True)

    for endpoint_path, token, expected_status in cases:
        status = _schemeshard_get_status(cluster, endpoint_path, token)
        assert status == expected_status, (
            f'Expected GET {endpoint_path} with token={_schemeshard_token_desc(token)} '
            f'to return {expected_status}, got {status}'
        )


def test_schemeshard_page_access_matrix_with_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet
    tid = cluster.schemeshard_tablet_id
    cases = _schemeshard_public_page_access_cases(tid) + _schemeshard_admin_page_access_cases(tid)

    for endpoint_path, token, expected_status in cases:
        status = _schemeshard_get_status(cluster, endpoint_path, token)
        assert status == expected_status, (
            f'Expected GET {endpoint_path} with token={_schemeshard_token_desc(token)} '
            f'to return {expected_status}, got {status}'
        )


def test_schemeshard_post_force_drop_unsafe_with_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet
    tid = cluster.schemeshard_tablet_id
    post_data = 'OwnerPathId=1&LocalPathId=1'
    legacy_path, secure_path, admin_secure_status = _schemeshard_post_action_paths(
        tid,
        'ForceDropUnsafe',
        'OwnerPathId=1&LocalPathId=1',
        admin_secure_status=400,
    )
    cases = _schemeshard_post_access_cases(legacy_path, secure_path, admin_secure_status)

    for endpoint_path, token, expected_status in cases:
        status = _schemeshard_post_status(cluster, endpoint_path, token, post_data)
        assert status == expected_status, (
            f'Expected POST {endpoint_path} with token={_schemeshard_token_desc(token)} '
            f'to return {expected_status}, got {status}'
        )


def test_schemeshard_post_split_one_to_one_with_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet
    tid = cluster.schemeshard_tablet_id
    post_data = 'ShardID=1'
    legacy_path, secure_path, admin_secure_status = _schemeshard_post_action_paths(
        tid,
        'SplitOneToOne',
        admin_secure_status=400,
    )
    cases = _schemeshard_post_access_cases(legacy_path, secure_path, admin_secure_status)

    for endpoint_path, token, expected_status in cases:
        status = _schemeshard_post_status(cluster, endpoint_path, token, post_data)
        assert status == expected_status, (
            f'Expected POST {endpoint_path} with token={_schemeshard_token_desc(token)} '
            f'to return {expected_status}, got {status}'
        )


def test_schemeshard_new_post_action_is_admin_only_by_default(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet
    tid = cluster.schemeshard_tablet_id
    legacy_path, secure_path, admin_secure_status = _schemeshard_post_action_paths(
        tid,
        'FutureAction',
        admin_secure_status=400,
    )
    cases = _schemeshard_post_access_cases(legacy_path, secure_path, admin_secure_status)

    for endpoint_path, token, expected_status in cases:
        status = _schemeshard_post_status(cluster, endpoint_path, token)
        assert status == expected_status, (
            f'Expected POST {endpoint_path} with token={_schemeshard_token_desc(token)} '
            f'to return {expected_status}, got {status}'
        )


def test_schemeshard_post_table_partitions_format_switch_with_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet
    tid = cluster.schemeshard_tablet_id
    post_data = 'OwnerPathId=1&LocalPathId=1&format=shardidx'
    legacy_path, secure_path, admin_secure_status = _schemeshard_post_action_paths(
        tid,
        'TablePartitionsFormatSwitch',
        'OwnerPathId=1&LocalPathId=1&format=shardidx',
        admin_secure_status=400,
    )
    cases = _schemeshard_post_access_cases(legacy_path, secure_path, admin_secure_status)

    for endpoint_path, token, expected_status in cases:
        status = _schemeshard_post_status(cluster, endpoint_path, token, post_data)
        assert status == expected_status, (
            f'Expected POST {endpoint_path} with token={_schemeshard_token_desc(token)} '
            f'to return {expected_status}, got {status}'
        )


def test_schemeshard_post_table_partitions_format_sweep_with_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet
    tid = cluster.schemeshard_tablet_id
    post_data = 'Start=1&format=shardidx'
    legacy_path, secure_path, admin_secure_status = _schemeshard_post_action_paths(
        tid,
        'TablePartitionsFormatSweep',
        page='Admin',
        admin_secure_status=303,
    )
    cases = _schemeshard_post_access_cases(legacy_path, secure_path, admin_secure_status)

    for endpoint_path, token, expected_status in cases:
        status = _schemeshard_post_status(cluster, endpoint_path, token, post_data)
        assert status == expected_status, (
            f'Expected POST {endpoint_path} with token={_schemeshard_token_desc(token)} '
            f'to return {expected_status}, got {status}'
        )


def test_schemeshard_table_partitions_format_sweep_form_uses_secure_path(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet
    tid = cluster.schemeshard_tablet_id
    endpoint_path = f'/tablets/app/secure?TabletID={tid}&Page=Admin'
    response = _schemeshard_get_response(cluster, endpoint_path, 'root@builtin')

    assert response.status_code == 200, response.text
    assert "action='app/secure?" in response.text or 'action="app/secure?' in response.text
    assert 'Action=TablePartitionsFormatSweep' in response.text


def test_schemeshard_admin_link_uses_secure_path(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet
    tid = cluster.schemeshard_tablet_id
    endpoint_path = f'/tablets/app?TabletID={tid}'
    response = _schemeshard_get_response(cluster, endpoint_path, 'monitoring@builtin')

    assert response.status_code == 200, response.text
    assert 'app/secure?' in response.text
    assert 'Page=Admin' in response.text


def test_schemeshard_force_drop_unsafe_form_uses_secure_path(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet
    tid = cluster.schemeshard_tablet_id
    endpoint_path = f'/tablets/app?TabletID={tid}' f'&Page=PathInfo&OwnerPathId={tid}&LocalPathId=1'
    response = _schemeshard_get_response(cluster, endpoint_path, 'monitoring@builtin')

    assert response.status_code == 200, response.text
    assert "action='app/secure?" in response.text or 'action="app/secure?' in response.text
    assert 'Action=ForceDropUnsafe' in response.text


def test_schemeshard_new_action_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token_and_schemeshard_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_and_schemeshard_tablet
    tid = cluster.schemeshard_tablet_id
    cases = _schemeshard_new_action_cases(tid, 'Page=FuturePage', secure_path_mode=False)

    for endpoint_path, token, expected_status in cases:
        status = _schemeshard_get_status(cluster, endpoint_path, token)
        assert status == expected_status, (
            f'Expected GET {endpoint_path} with token={_schemeshard_token_desc(token)} '
            f'to return {expected_status}, got {status}'
        )


def test_schemeshard_new_action_with_enforce_user_token_and_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet
    tid = cluster.schemeshard_tablet_id
    cases = _schemeshard_new_action_cases(tid, 'Page=FuturePage', secure_path_mode=True)

    for endpoint_path, token, expected_status in cases:
        status = _schemeshard_get_status(cluster, endpoint_path, token)
        assert status == expected_status, (
            f'Expected GET {endpoint_path} with token={_schemeshard_token_desc(token)} '
            f'to return {expected_status}, got {status}'
        )


def _bscontroller_endpoint_cases(endpoint_paths, token_statuses):
    return [
        (endpoint_path, token, expected_status)
        for endpoint_path in endpoint_paths
        for token, expected_status in token_statuses.items()
    ]


def _bscontroller_token_desc(token):
    return token if token is not None else 'null'


def _bscontroller_mon_base_url(cluster):
    node = cluster.nodes[1]
    return f'https://{node.host}:{node.mon_port}'


def _bscontroller_get_status(cluster, endpoint_path, token=None):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    response = requests.get(
        f'{_bscontroller_mon_base_url(cluster)}{endpoint_path}',
        headers=headers,
        verify=False,
    )
    return response.status_code


_BSCONTROLLER_PAGES = (
    '',  # main page
    'page=GetDown',
    'page=OperationLog',
    'page=OperationLogEntry',
    'page=HealthEvents',
    'page=SelfHeal',
    'page=Groups',
    'page=GroupDetail',
    'page=Scrub',
    'page=Shred',
    'page=InternalTables',
    'page=Bridge',
    'page=VirtualGroups',
    'page=SetDown&group=0&down=1',
    'page=SelfHeal&disable=1&action=disableSelfHeal',
    'page=Shred&startshred=1&generation=0',
    'page=StopGivingGroups',
    'page=StartGivingGroups',  # must follow StopGivingGroups: restores group allocation
    'page=NewAction',
)


def _bscontroller_devui_cases(tablet_id, secure_path_mode):
    q_base = f'TabletID={tablet_id}'
    all_forbidden, monitoring_allowed, admin_allowed = tablet_devui_sid_matrix()
    expected_on_app = tablet_devui_expected_on_app(secure_path_mode, monitoring_allowed, all_forbidden)
    cases = []
    for query_suffix in _BSCONTROLLER_PAGES:
        q = q_base if not query_suffix else f'{q_base}&{query_suffix}'
        cases.extend(_bscontroller_endpoint_cases([f'/tablets/app?{q}'], expected_on_app))
        cases.extend(_bscontroller_endpoint_cases([f'/tablets/app/secure?{q}'], admin_allowed))
    # Tablets summary page is a different handler and keeps monitoring-level access.
    cases.extend(_bscontroller_endpoint_cases([f'/tablets?{q_base}'], monitoring_allowed))
    return cases


def _bscontroller_post_exec_paths(tablet_id):
    q = f'TabletID={tablet_id}&exec=1'
    all_forbidden, _, admin_allowed_sids_ok = tablet_devui_sid_matrix()
    return {
        f'/tablets/app?{q}': all_forbidden,
        f'/tablets/app/secure?{q}': admin_allowed_sids_ok,
    }


def _bscontroller_tablet_devui_mon_paths(cluster, secure_path_mode):
    for endpoint_path, token, expected_status in _bscontroller_devui_cases(
        BSC_TABLET_ID, secure_path_mode=secure_path_mode
    ):
        status = _bscontroller_get_status(cluster, endpoint_path, token)
        assert status == expected_status, (
            f'Expected GET {endpoint_path} with token={_bscontroller_token_desc(token)} '
            f'to return {expected_status}, got {status}'
        )


def test_bscontroller_tablet_devui_mon_paths_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token,
):
    cluster = ydb_cluster_with_enforce_user_token
    _bscontroller_tablet_devui_mon_paths(cluster, False)


def test_bscontroller_tablet_devui_mon_paths_with_enforce_user_token_and_secure_path_mode(
    ydb_cluster_with_enforce_user_token_and_tablet_devui_secure_path_flag,
):
    cluster = ydb_cluster_with_enforce_user_token_and_tablet_devui_secure_path_flag
    _bscontroller_tablet_devui_mon_paths(cluster, True)


def test_bscontroller_post_exec_with_enforce_user_token_and_secure_path_mode(
    ydb_cluster_with_enforce_user_token_and_tablet_devui_secure_path_flag,
):
    cluster = ydb_cluster_with_enforce_user_token_and_tablet_devui_secure_path_flag
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    base_url = f'https://{host}:{mon_port}'
    for endpoint_path, expected_statuses in _bscontroller_post_exec_paths(BSC_TABLET_ID).items():
        endpoint_url = f'{base_url}{endpoint_path}'
        for token, expected_status in expected_statuses.items():
            headers = {'Content-Type': 'application/json'}
            if token is not None:
                headers['Authorization'] = token
            response = requests.post(endpoint_url, headers=headers, data='{}', verify=False)
            token_desc = token if token is not None else 'null'
            if endpoint_path.startswith('/tablets/app/secure') and token == 'root@builtin':
                # Auth passed; empty config body may be rejected later with 400.
                assert response.status_code in (200, 400), (
                    f'Expected POST {endpoint_path} with token={token_desc} to pass auth, got {response.status_code}'
                )
            else:
                assert response.status_code == expected_status, (
                    f'Expected POST {endpoint_path} with token={token_desc} to return {expected_status}, '
                    f'got {response.status_code}'
                )


def _bscontroller_has_hardcoded_app_path(text):
    for attr in ("href='app", 'href="app', "action='app", 'action="app'):
        if attr in text:
            return True
    return False


def test_bscontroller_links_and_forms_stay_on_current_app_path(
    ydb_cluster_with_enforce_user_token_and_tablet_devui_secure_path_flag,
):
    cluster = ydb_cluster_with_enforce_user_token_and_tablet_devui_secure_path_flag
    base_url = _bscontroller_mon_base_url(cluster)
    headers = {'Authorization': 'root@builtin'}

    def get(query):
        response = requests.get(
            f'{base_url}/tablets/app/secure?TabletID={BSC_TABLET_ID}{query}',
            headers=headers,
            verify=False,
        )
        assert response.status_code == 200, response.text
        return response.text

    main_page = get('')
    assert f"href='?TabletID={BSC_TABLET_ID}&page=OperationLog'" in main_page
    assert f"href='?TabletID={BSC_TABLET_ID}&page=SelfHeal'" in main_page
    assert not _bscontroller_has_hardcoded_app_path(main_page)

    internal_tables = get('&page=InternalTables')
    assert f"href='?TabletID={BSC_TABLET_ID}&page=InternalTables&table=pdisks'" in internal_tables
    assert not _bscontroller_has_hardcoded_app_path(internal_tables)

    shred = get('&page=Shred')
    assert "name='startshred'" in shred or 'name="startshred"' in shred
    assert not _bscontroller_has_hardcoded_app_path(shred)

    self_heal = get('&page=SelfHeal')
    assert not _bscontroller_has_hardcoded_app_path(self_heal)

    disable_self_heal = get('&page=SelfHeal&disable=1&action=disableSelfHeal')
    assert f'content="0; ?TabletID={BSC_TABLET_ID}&page=SelfHeal"' in disable_self_heal
    assert 'content="0; app' not in disable_self_heal


def test_bscontroller_new_action_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token,
):
    _test_endpoints(
        ydb_cluster_with_enforce_user_token,
        tablet_devui_new_action_paths(BSC_TABLET_ID, 'page=NewAction', secure_path_mode=False),
    )


def test_bscontroller_new_action_with_enforce_user_token_and_secure_path_mode(
    ydb_cluster_with_enforce_user_token_and_tablet_devui_secure_path_flag,
):
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_and_tablet_devui_secure_path_flag,
        tablet_devui_new_action_paths(BSC_TABLET_ID, 'page=NewAction', secure_path_mode=True),
    )


def _graph_shard_devui_mon_paths(graph_shard_tablet_id, secure_path_mode):
    q = f'TabletID={graph_shard_tablet_id}'
    all_forbidden, monitoring_allowed_sids_ok, admin_allowed_sids_ok = tablet_devui_sid_matrix()
    expected_on_app = tablet_devui_expected_on_app(secure_path_mode, monitoring_allowed_sids_ok, all_forbidden)
    paths = {
        f'/tablets/app?{q}': monitoring_allowed_sids_ok,
        f'/tablets?{q}': monitoring_allowed_sids_ok,
        f'/tablets/app?{q}&action=get_settings': monitoring_allowed_sids_ok,
        f'/tablets/app?{q}&action=change_backend&backend=1': expected_on_app,
    }
    if secure_path_mode:
        paths[f'/tablets/app/secure?{q}'] = admin_allowed_sids_ok
        paths[f'/tablets/app/secure?{q}&action=change_backend&backend=1'] = admin_allowed_sids_ok
    return paths


def test_graph_shard_devui_mon_paths_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token_and_graph_shard,
):
    tid = ydb_cluster_with_enforce_user_token_and_graph_shard.graph_shard_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_and_graph_shard,
        _graph_shard_devui_mon_paths(tid, secure_path_mode=False),
    )


def test_graph_shard_devui_mon_paths_with_enforce_user_token_and_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_graph_shard,
):
    tid = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_graph_shard.graph_shard_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_secure_devui_flag_and_graph_shard,
        _graph_shard_devui_mon_paths(tid, secure_path_mode=True),
    )


def test_graph_shard_new_action_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token_and_graph_shard,
):
    tid = ydb_cluster_with_enforce_user_token_and_graph_shard.graph_shard_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_and_graph_shard,
        tablet_devui_new_action_paths(tid, 'NewPage=1', secure_path_mode=False),
    )


def test_graph_shard_new_action_with_enforce_user_token_and_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_graph_shard,
):
    tid = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_graph_shard.graph_shard_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_secure_devui_flag_and_graph_shard,
        tablet_devui_new_action_paths(tid, 'NewPage=1', secure_path_mode=True),
    )


def test_graph_shard_change_backend_links_use_secure_path(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_graph_shard,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_graph_shard
    tid = cluster.graph_shard_tablet_id
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    url = f'https://{host}:{mon_port}/tablets/app?TabletID={tid}'
    response = requests.get(url, headers={'Authorization': 'monitoring@builtin'}, verify=False)
    assert response.status_code == 200, response.text
    assert 'app/secure?' in response.text
    assert 'action=change_backend&backend=0' in response.text
    assert 'action=change_backend&backend=1' in response.text
    assert 'action=change_backend&backend=2' in response.text
    assert f'app?TabletID={tid}&action=change_backend' not in response.text


def _pers_queue_endpoint_cases(endpoint_paths, token_statuses):
    return [
        (endpoint_path, token, expected_status)
        for endpoint_path in endpoint_paths
        for token, expected_status in token_statuses.items()
    ]


def _pers_queue_token_desc(token):
    return token if token is not None else 'null'


def _pers_queue_mon_base_url(cluster):
    node = cluster.nodes[1]
    return f'https://{node.host}:{node.mon_port}'


def _pers_queue_get_status(cluster, endpoint_path, token=None):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    response = requests.get(
        f'{_pers_queue_mon_base_url(cluster)}{endpoint_path}',
        headers=headers,
        verify=False,
    )
    return response.status_code


# Views whose every parameter is in the public whitelist, so they keep monitoring-level access.
_PERS_QUEUE_PUBLIC_PAGES = (
    '',  # main page
    'kv=1',
    'kv=1&section=channelstat',
    'consumer=user&partitionId=0',
    'TxId=1',
)

# SendReadSet commits or aborts a transaction. NewAction stands for a handler added later: an
# unknown parameter is admin only without anyone having to remember to list it.
_PERS_QUEUE_ADMIN_PAGES = (
    'SendReadSet=1&step=1&txId=1&decision=commit&allSenderTablets=1',
    'SendReadSet=1&step=1&txId=1&decision=abort&senderTablet=1',
    'NewAction=1',
)


def _pers_queue_devui_cases(tablet_id, secure_path_mode):
    q_base = f'TabletID={tablet_id}'
    all_forbidden, monitoring_allowed, admin_allowed = tablet_devui_sid_matrix()
    expected_on_app = tablet_devui_expected_on_app(secure_path_mode, monitoring_allowed, all_forbidden)
    cases = []
    for query_suffix in _PERS_QUEUE_PUBLIC_PAGES:
        q = q_base if not query_suffix else f'{q_base}&{query_suffix}'
        cases.extend(_pers_queue_endpoint_cases([f'/tablets/app?{q}'], monitoring_allowed))
        cases.extend(_pers_queue_endpoint_cases([f'/tablets/app/secure?{q}'], admin_allowed))
    for query_suffix in _PERS_QUEUE_ADMIN_PAGES:
        q = f'{q_base}&{query_suffix}'
        cases.extend(_pers_queue_endpoint_cases([f'/tablets/app?{q}'], expected_on_app))
        cases.extend(_pers_queue_endpoint_cases([f'/tablets/app/secure?{q}'], admin_allowed))
    # Tablets summary page is a different handler and keeps monitoring-level access.
    cases.extend(_pers_queue_endpoint_cases([f'/tablets?{q_base}'], monitoring_allowed))
    return cases


def _pers_queue_tablet_devui_mon_paths(cluster, secure_path_mode):
    for endpoint_path, token, expected_status in _pers_queue_devui_cases(
        cluster.pers_queue_tablet_id, secure_path_mode=secure_path_mode
    ):
        status = _pers_queue_get_status(cluster, endpoint_path, token)
        assert status == expected_status, (
            f'Expected GET {endpoint_path} with token={_pers_queue_token_desc(token)} '
            f'to return {expected_status}, got {status}'
        )


def test_pers_queue_tablet_devui_mon_paths_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token_and_pers_queue_topic,
):
    _pers_queue_tablet_devui_mon_paths(
        ydb_cluster_with_enforce_user_token_and_pers_queue_topic, secure_path_mode=False
    )


def test_pers_queue_tablet_devui_mon_paths_with_enforce_user_token_and_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_pers_queue_topic,
):
    _pers_queue_tablet_devui_mon_paths(
        ydb_cluster_with_enforce_user_token_secure_devui_flag_and_pers_queue_topic, secure_path_mode=True
    )


def test_pers_queue_send_read_set_form_points_to_secure_path(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_pers_queue_topic,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_pers_queue_topic
    tid = cluster.pers_queue_tablet_id
    base_url = _pers_queue_mon_base_url(cluster)

    on_app = requests.get(
        f'{base_url}/tablets/app?TabletID={tid}&TxId=1',
        headers={'Authorization': 'monitoring@builtin'},
        verify=False,
    )
    assert on_app.status_code == 200, on_app.text
    assert f"action='app/secure?TabletID={tid}'" not in on_app.text or 'SendReadSet' in on_app.text

    on_secure = requests.get(
        f'{base_url}/tablets/app/secure?TabletID={tid}&TxId=1',
        headers={'Authorization': 'root@builtin'},
        verify=False,
    )
    assert on_secure.status_code == 200, on_secure.text
    assert "action='app" not in on_secure.text and 'action="app' not in on_secure.text
