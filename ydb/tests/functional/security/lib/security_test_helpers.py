# -*- coding: utf-8 -*-
import logging
from contextlib import contextmanager

import requests

from ydb.tests.library.common.wait_for import wait_for

logger = logging.getLogger(__name__)

DATABASE = '/Root'


def tablet_devui_sid_matrix():
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
    return all_forbidden, monitoring_allowed_sids_ok, admin_allowed_sids_ok


def tablet_devui_expected_on_app(secure_path_mode, monitoring_ok, all_forbidden):
    return all_forbidden if secure_path_mode else monitoring_ok


def tablet_devui_new_action_paths(tablet_id, query_suffix, secure_path_mode):
    all_forbidden, monitoring_ok, admin_ok = tablet_devui_sid_matrix()
    expected_on_app = tablet_devui_expected_on_app(secure_path_mode, monitoring_ok, all_forbidden)
    q = f'TabletID={tablet_id}'
    return {
        f'/tablets/app?{q}&{query_suffix}': expected_on_app,
        f'/tablets/app/secure?{q}&{query_suffix}': admin_ok,
    }


def _test_endpoint(endpoint_url, endpoint_path, token, expected_status):
    headers = {}
    if token is not None:
        headers["Authorization"] = token
    response = requests.get(endpoint_url, headers=headers, verify=False)
    token_desc = token if token is not None else "null"
    assert (
        response.status_code == expected_status
    ), f"Expected {endpoint_path} with token={token_desc} to return {expected_status}, got {response.status_code}"


def _test_endpoints(cluster, expected_results):
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    base_url = f"https://{host}:{mon_port}"

    for endpoint_path, expected_statuses in expected_results.items():
        endpoint_url = f"{base_url}{endpoint_path}"
        for token, expected_status in expected_statuses.items():
            _test_endpoint(endpoint_url, endpoint_path, token, expected_status)


def _test_endpoints_via_node_proxy(node, path_suffix, expected_statuses_by_token):
    base_url = f"https://{node.host}:{node.mon_port}"
    node_id = node.node_id
    full_path = f"/node/{node_id}{path_suffix}"
    endpoint_url = f"{base_url}{full_path}"
    for token, expected_status in expected_statuses_by_token.items():
        _test_endpoint(endpoint_url, full_path, token, expected_status)


def mon_base_url(cluster, node_index=1):
    node = cluster.nodes[node_index]
    return f'https://{node.host}:{node.mon_port}'


def describe_path_self(cluster, root_path, database_path, use_tls=False, token=None):
    node = cluster.nodes[1]
    scheme = 'https' if use_tls else 'http'
    response = requests.get(
        f'{scheme}://{node.host}:{node.mon_port}/viewer/json/describe',
        params={'database': root_path, 'path': database_path},
        headers={'Authorization': token} if token is not None else {},
        verify=False,
        timeout=5,
    )
    response.raise_for_status()
    return response.json()['PathDescription']['Self']


def get_tenant_schemeshard_id(cluster, root_path, database_path, use_tls=False, token=None):
    return int(describe_path_self(cluster, root_path, database_path, use_tls, token)['SchemeshardId'])


def get_tenant_path_id(cluster, root_path, database_path, use_tls=False, token=None):
    return int(describe_path_self(cluster, root_path, database_path, use_tls, token)['PathId'])


def get_nodelist_ids(base_url, database=None, token='root@builtin'):
    params = {}
    if database is not None:
        params['database'] = database
    response = requests.get(
        base_url + '/viewer/json/nodelist',
        params=params,
        headers={'Authorization': token},
        verify=False,
        timeout=5,
    )
    response.raise_for_status()
    return [node['Id'] for node in response.json()]


def get_foreign_node_id_for_database(base_url, database, token='root@builtin'):
    database_nodes = set(get_nodelist_ids(base_url, database=database, token=token))
    foreign_nodes = set(get_nodelist_ids(base_url, token=token)) - database_nodes
    assert foreign_nodes, f'no foreign nodes found for database={database}'
    return min(foreign_nodes)


def get_storage_groups(base_url, database=None, token='root@builtin', timeout=30, **params):
    if database is not None:
        params = {'database': database, **params}
    response = requests.get(
        base_url + '/storage/groups',
        params=params,
        headers={'Authorization': token},
        verify=False,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def get_storage_ids(base_url, database=None, token='root@builtin', timeout=60):
    """Ids covered by the storage groups of the given database, or of the whole cluster
    when database is None: the groups themselves and the nodes/pdisks holding their vdisks."""
    data = get_storage_groups(
        base_url,
        database,
        token,
        fields_required='GroupId,VDisk,PDisk,NodeId,PDiskId',
        timeout=timeout,
    )
    ids = {'group_ids': set(), 'node_ids': set(), 'pdisk_ids': set()}
    for group in data.get('StorageGroups') or []:
        ids['group_ids'].add(int(group['GroupId']))
        for vdisk in group.get('VDisks') or []:
            # PDiskId is reported as "<node_id>-<pdisk_id>"
            pdisk_id_str = str((vdisk.get('PDisk') or {}).get('PDiskId') or '')
            if '-' in pdisk_id_str:
                node_id_str, _, local_pdisk_id_str = pdisk_id_str.partition('-')
                ids['node_ids'].add(int(node_id_str))
                ids['pdisk_ids'].add(int(local_pdisk_id_str))
    return ids


def wait_for_storage_ids(base_url, database, token='root@builtin', timeout_seconds=60):
    """Same as get_storage_ids, but waits until the database gets its storage groups."""
    last = {}

    def ready():
        last['ids'] = get_storage_ids(base_url, database, token)
        return bool(last['ids']['node_ids'])

    if not wait_for(ready, timeout_seconds=timeout_seconds, step_seconds=1):
        raise AssertionError(
            f'no storage groups with disks for database={database} after {timeout_seconds}s; '
            f'last={last.get("ids")}'
        )
    return last['ids']


def get_unknown_node_id(base_url, token='root@builtin'):
    """Returns a node id that does not exist in the cluster at all."""
    return max(get_nodelist_ids(base_url, token=token)) + 100


def wait_for_viewer_ready(
    base_url,
    database=DATABASE,
    token='root@builtin',
    timeout_seconds=30,
    verify=False,
):
    """Wait until viewer HTTP handlers are registered and responding."""

    last_failure = {"status": None, "exc": None}

    def ready():
        try:
            headers = {}
            if token is not None:
                headers["Authorization"] = token
            response = requests.post(
                base_url + '/viewer/query',
                headers=headers,
                params={'database': database, 'query': 'SELECT 1;', 'schema': 'multi'},
                verify=verify,
                timeout=5,
            )
            if response.status_code == 200:
                return True
            last_failure["status"] = response.status_code
            last_failure["exc"] = None
            logger.info(
                'Viewer not ready yet at %s: /viewer/query returned %s %s',
                base_url,
                response.status_code,
                response.text,
            )
            return False
        except requests.RequestException as exc:
            last_failure["status"] = None
            last_failure["exc"] = str(exc)
            logger.info('Viewer not ready yet at %s: %s', base_url, exc)
            return False

    if not wait_for(ready, timeout_seconds=timeout_seconds, step_seconds=1):
        raise AssertionError(
            f'Viewer at {base_url} is not ready after {timeout_seconds}s; '
            f'last_status={last_failure["status"]}; last_error={last_failure["exc"]}'
        )


def run_viewer_query(base_url, query, database=DATABASE, token='root@builtin', timeout=5):
    response = requests.post(
        base_url + '/viewer/query',
        headers={'Authorization': token},
        params={'database': database, 'query': query, 'schema': 'multi'},
        verify=False,
        timeout=timeout,
    )
    assert response.status_code == 200, response.text
    return response


@contextmanager
def grants_provided(base_url, object_path, *permissions, database=DATABASE):
    grantees = '`database@builtin`, `viewer@builtin`, `monitoring@builtin`'
    perms = ', '.join(f"'{permission}'" for permission in permissions)
    run_viewer_query(
        base_url,
        f"GRANT {perms} ON `{object_path}` TO {grantees};",
        database=database,
    )
    try:
        yield
    finally:
        run_viewer_query(
            base_url,
            f"REVOKE {perms} ON `{object_path}` FROM {grantees};",
            database=database,
        )


def grant_describe_schema_provided(base_url, database=DATABASE):
    return grants_provided(base_url, database, 'ydb.granular.describe_schema', database=database)


@contextmanager
def with_topic(base_url, topic_name, database=DATABASE):
    run_viewer_query(base_url, f'CREATE TOPIC `{topic_name}`;', database=database)
    try:
        yield f'{database}/{topic_name}'
    finally:
        run_viewer_query(base_url, f'DROP TOPIC `{topic_name}`;', database=database)
