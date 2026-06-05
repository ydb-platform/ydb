# -*- coding: utf-8 -*-
import time

import pytest
import requests

from security_test_helpers import (
    _test_endpoints,
    tablet_devui_expected_on_app,
    tablet_devui_new_action_paths,
    tablet_devui_sid_matrix,
)
from ydb.tests.oss.ydb_sdk_import import ydb


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


def _prepare_datashard_tablet(cluster):
    database = '/Root/ds_mon_security'
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
    all_forbidden = {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 403,
        'root@builtin': 403,
    }
    monitoring_allowed_sids_ok = {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    }
    admin_allowed_sids_ok = {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 403,
        'root@builtin': 200,
    }
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




def _schemeshard_devui_mon_paths(tablet_id, secure_path_mode):
    q = f'TabletID={tablet_id}'
    all_forbidden, monitoring_allowed_sids_ok, admin_allowed_sids_ok = tablet_devui_sid_matrix()
    return {
        f'/tablets/app?{q}': monitoring_allowed_sids_ok,
        f'/tablets?{q}': monitoring_allowed_sids_ok,
        f'/tablets/app?{q}&Page=Admin': tablet_devui_expected_on_app(secure_path_mode, monitoring_allowed_sids_ok, all_forbidden),
        f'/tablets/app/secure?{q}&Page=Admin': admin_allowed_sids_ok,
        f'/tablets/app?{q}&Page=AdminRequest&UpdateAccessDatabaseRights=1&UpdateAccessDatabaseRightsDryRun=1': (
            tablet_devui_expected_on_app(secure_path_mode, monitoring_allowed_sids_ok, all_forbidden)
        ),
        f'/tablets/app/secure?{q}&Page=AdminRequest&UpdateAccessDatabaseRights=1&UpdateAccessDatabaseRightsDryRun=1': (
            admin_allowed_sids_ok
        ),
    }


def _schemeshard_post_action_paths(tablet_id, action, extra_params=''):
    q = f'TabletID={tablet_id}&Action={action}'
    if extra_params:
        q = f'{q}&{extra_params}'
    all_forbidden, _, admin_allowed_sids_ok = tablet_devui_sid_matrix()
    return {
        f'/tablets/app?{q}': all_forbidden,
        f'/tablets/app/secure?{q}': admin_allowed_sids_ok,
    }


def test_schemeshard_tablet_devui_mon_paths_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token_and_schemeshard_tablet,
):
    tid = ydb_cluster_with_enforce_user_token_and_schemeshard_tablet.schemeshard_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_and_schemeshard_tablet,
        _schemeshard_devui_mon_paths(tid, secure_path_mode=False),
    )


def test_schemeshard_tablet_devui_mon_paths_with_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet,
):
    tid = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet.schemeshard_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet,
        _schemeshard_devui_mon_paths(tid, secure_path_mode=True),
    )


def test_schemeshard_post_force_drop_unsafe_with_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    base_url = f'https://{host}:{mon_port}'
    tid = cluster.schemeshard_tablet_id
    for endpoint_path, expected_statuses in _schemeshard_post_action_paths(
        tid,
        'ForceDropUnsafe',
        'OwnerPathId=1&LocalPathId=1',
    ).items():
        endpoint_url = f'{base_url}{endpoint_path}'
        for token, expected_status in expected_statuses.items():
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            if token is not None:
                headers['Authorization'] = token
            response = requests.post(
                endpoint_url,
                headers=headers,
                data='OwnerPathId=1&LocalPathId=1',
                verify=False,
            )
            token_desc = token if token is not None else 'null'
            if endpoint_path.startswith('/tablets/app/secure') and token == 'root@builtin':
                assert response.status_code in (200, 400, 502), (
                    f'Expected POST {endpoint_path} with token={token_desc} to pass auth, got {response.status_code}'
                )
            else:
                assert response.status_code == expected_status, (
                    f'Expected POST {endpoint_path} with token={token_desc} to return {expected_status}, '
                    f'got {response.status_code}'
                )


def test_schemeshard_post_split_one_to_one_with_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    base_url = f'https://{host}:{mon_port}'
    tid = cluster.schemeshard_tablet_id
    for endpoint_path, expected_statuses in _schemeshard_post_action_paths(tid, 'SplitOneToOne').items():
        endpoint_url = f'{base_url}{endpoint_path}'
        for token, expected_status in expected_statuses.items():
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            if token is not None:
                headers['Authorization'] = token
            response = requests.post(
                endpoint_url,
                headers=headers,
                data='ShardID=1',
                verify=False,
            )
            token_desc = token if token is not None else 'null'
            if endpoint_path.startswith('/tablets/app/secure') and token == 'root@builtin':
                assert response.status_code in (200, 400), (
                    f'Expected POST {endpoint_path} with token={token_desc} to pass auth, got {response.status_code}'
                )
            else:
                assert response.status_code == expected_status, (
                    f'Expected POST {endpoint_path} with token={token_desc} to return {expected_status}, '
                    f'got {response.status_code}'
                )


def test_schemeshard_new_post_action_is_admin_only_by_default(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    base_url = f'https://{host}:{mon_port}'
    tid = cluster.schemeshard_tablet_id
    for endpoint_path, expected_statuses in _schemeshard_post_action_paths(tid, 'FutureAction').items():
        endpoint_url = f'{base_url}{endpoint_path}'
        for token, expected_status in expected_statuses.items():
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            if token is not None:
                headers['Authorization'] = token
            response = requests.post(endpoint_url, headers=headers, data='', verify=False)
            token_desc = token if token is not None else 'null'
            if endpoint_path.startswith('/tablets/app/secure') and token == 'root@builtin':
                # Auth passed; the tablet rejects the unknown action afterwards.
                assert response.status_code == 400, (
                    f'Expected POST {endpoint_path} with token={token_desc} to pass auth, got {response.status_code}'
                )
            else:
                assert response.status_code == expected_status, (
                    f'Expected POST {endpoint_path} with token={token_desc} to return {expected_status}, '
                    f'got {response.status_code}'
                )


def test_schemeshard_admin_link_uses_secure_path(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    tid = cluster.schemeshard_tablet_id
    url = f'https://{host}:{mon_port}/tablets/app?TabletID={tid}'
    response = requests.get(url, headers={'Authorization': 'monitoring@builtin'}, verify=False)
    assert response.status_code == 200, response.text
    assert 'app/secure?' in response.text
    assert 'Page=Admin' in response.text


def test_schemeshard_force_drop_unsafe_form_uses_secure_path(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet,
):
    cluster = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard_tablet
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    tid = cluster.schemeshard_tablet_id
    url = (
        f'https://{host}:{mon_port}/tablets/app?TabletID={tid}'
        f'&Page=PathInfo&OwnerPathId={tid}&LocalPathId=1'
    )
    response = requests.get(url, headers={'Authorization': 'monitoring@builtin'}, verify=False)
    assert response.status_code == 200, response.text
    assert "action='app/secure?" in response.text or 'action="app/secure?' in response.text
    assert 'Action=ForceDropUnsafe' in response.text

def test_schemeshard_new_action_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token_and_schemeshard,
):
    tid = ydb_cluster_with_enforce_user_token_and_schemeshard.schemeshard_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_and_schemeshard,
        tablet_devui_new_action_paths(tid, 'Page=FuturePage', secure_path_mode=False),
    )


def test_schemeshard_new_action_with_enforce_user_token_and_secure_path_mode(
    ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard,
):
    tid = ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard.schemeshard_tablet_id
    _test_endpoints(
        ydb_cluster_with_enforce_user_token_secure_devui_flag_and_schemeshard,
        tablet_devui_new_action_paths(tid, 'Page=FuturePage', secure_path_mode=True),
    )
