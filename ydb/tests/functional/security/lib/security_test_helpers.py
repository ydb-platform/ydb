# -*- coding: utf-8 -*-
from contextlib import contextmanager

import requests

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
