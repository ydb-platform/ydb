# -*- coding: utf-8 -*-
from urllib.parse import urlencode

import requests

requests.packages.urllib3.disable_warnings()

DATABASE = '/Root'
TOPIC_NAME = 't'
TOPIC_PATH = f'{DATABASE}/{TOPIC_NAME}'


def _get_status(base_url, path, token, timeout=1):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    response = requests.get(base_url + path, headers=headers, verify=False, timeout=timeout)
    return response.status_code


def _post_status(base_url, path, token, timeout=1):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    response = requests.post(base_url + path, headers=headers, verify=False, timeout=timeout)
    return response.status_code


def _assert_status(base_url, path, token, status, timeout=1):
    assert _get_status(base_url, path, token, timeout=timeout) == status
    assert _post_status(base_url, path, token, timeout=timeout) == status


def _assert_not_status(base_url, path, token, status, timeout=1):
    assert _get_status(base_url, path, token, timeout=timeout) != status
    assert _post_status(base_url, path, token, timeout=timeout) != status


# External viewer access controls move these endpoints to viewer-level access.
def test_viewer_config_access_controls(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'

    for ep in ['/viewer/config', '/viewer/json/config']:
        _assert_status(base_url, ep, 'database@builtin', 403)
        _assert_not_status(base_url, ep, 'viewer@builtin', 403)
        _assert_not_status(base_url, ep, 'monitoring@builtin', 403)
        _assert_not_status(base_url, ep, 'root@builtin', 403)


def test_viewer_v2_aliases_access_controls(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    db_qs = f'?database={DATABASE.replace("/", "%2F")}'

    grant_query = (
        f"GRANT 'ydb.granular.describe_schema' ON `{DATABASE}` "
        f"TO `database@builtin`, `viewer@builtin`, `monitoring@builtin`;"
    )
    grant_response = requests.post(
        base_url + '/viewer/query',
        headers={'Authorization': 'root@builtin'},
        params={'database': DATABASE, 'query': grant_query, 'schema': 'multi'},
        verify=False,
        timeout=30,
    )
    assert grant_response.status_code == 200, grant_response.text

    for ep in ['/viewer/v2/json/config', '/viewer/v2/json/config' + db_qs]:
        # no access
        _assert_status(base_url, ep, 'database@builtin', 403)
        _assert_status(base_url, ep, 'viewer@builtin', 403)
        # with access
        _assert_status(base_url, ep, 'monitoring@builtin', 200)
        _assert_status(base_url, ep, 'root@builtin', 200)

    SYSINFO_V2_ENDPOINT = '/viewer/v2/json/sysinfo'
    # no database CGI-param for database_allowed_sids level
    _assert_status(base_url, SYSINFO_V2_ENDPOINT, 'database@builtin', 400)
    # with database CGI-param for database_allowed_sids level
    _assert_status(base_url, SYSINFO_V2_ENDPOINT + db_qs, 'database@builtin', 200)
    # check with and without database CGI-params for different access levels
    _assert_status(base_url, SYSINFO_V2_ENDPOINT, 'viewer@builtin', 200)
    _assert_status(base_url, SYSINFO_V2_ENDPOINT + db_qs, 'viewer@builtin', 200)
    _assert_status(base_url, SYSINFO_V2_ENDPOINT, 'monitoring@builtin', 200)
    _assert_status(base_url, SYSINFO_V2_ENDPOINT + db_qs, 'monitoring@builtin', 200)
    _assert_status(base_url, SYSINFO_V2_ENDPOINT + db_qs, 'root@builtin', 200)
    _assert_status(base_url, SYSINFO_V2_ENDPOINT, 'root@builtin', 200)


def _run_query(base_url, query, token='root@builtin'):
    response = requests.post(
        base_url + '/viewer/query',
        headers={'Authorization': token},
        params={'database': DATABASE, 'query': query, 'schema': 'multi'},
        verify=False,
        timeout=30,
    )
    assert response.status_code == 200, response.text
    return response


def _topic_data_path(endpoint, *, database=True):
    params = {
        'path': TOPIC_PATH,
        'partition': 0,
        'offset': 0,
        'limit': 1,
    }
    if database:
        params = {'database': DATABASE, **params}
    return endpoint + '?' + urlencode(params)


def test_database_scoped_endpoints_access_controls(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    db_qs = f'?database={DATABASE.replace("/", "%2F")}'

    grant_query = (
        f"GRANT 'ydb.granular.describe_schema' ON `{DATABASE}` "
        f"TO `database@builtin`, `viewer@builtin`, `monitoring@builtin`;"
    )
    grant_response = requests.post(
        base_url + '/viewer/query',
        headers={'Authorization': 'root@builtin'},
        params={'database': DATABASE, 'query': grant_query, 'schema': 'multi'},
        verify=False,
        timeout=30,
    )
    assert grant_response.status_code == 200, grant_response.text

    for ep in ['/viewer/sysinfo', '/viewer/json/sysinfo', '/viewer/json/feature_flags', '/viewer/feature_flags']:
        # no database CGI-param for database_allowed_sids level
        _assert_status(base_url, ep, 'database@builtin', 400)
        # with database CGI-param for database_allowed_sids level
        _assert_status(base_url, ep + db_qs, 'database@builtin', 200)
        # check with and without database CGI-params for different access levels
        _assert_status(base_url, ep, 'viewer@builtin', 200)
        _assert_status(base_url, ep + db_qs, 'viewer@builtin', 200)
        _assert_status(base_url, ep, 'monitoring@builtin', 200)
        _assert_status(base_url, ep + db_qs, 'monitoring@builtin', 200)
        _assert_status(base_url, ep + db_qs, 'root@builtin', 200)
        _assert_status(base_url, ep, 'root@builtin', 200)


def test_topic_data_access_controls(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    topic_data_timeout = 5

    _run_query(base_url, f'CREATE TOPIC `{TOPIC_NAME}`;')

    endpoints = ['/viewer/topic_data', '/viewer/json/topic_data']

    for ep in endpoints:
        for token in ('database@builtin', 'viewer@builtin', 'monitoring@builtin'):
            _assert_status(base_url, _topic_data_path(ep, database=True), token, 400, timeout=topic_data_timeout)
            _assert_status(base_url, _topic_data_path(ep, database=False), token, 400, timeout=topic_data_timeout)

    _run_query(
        base_url,
        f"GRANT 'ydb.granular.describe_schema' ON `{DATABASE}` "
        f"TO `database@builtin`, `viewer@builtin`, `monitoring@builtin`;"
    )
    _run_query(
        base_url,
        f"GRANT 'ydb.granular.describe_schema', 'ydb.granular.select_row' ON `{TOPIC_PATH}` "
        f"TO `database@builtin`, `viewer@builtin`, `monitoring@builtin`;"
    )

    for ep in endpoints:
        # no database CGI-param for database_allowed_sids level
        token = 'database@builtin'
        _assert_status(base_url, _topic_data_path(ep, database=False), token, 400, timeout=topic_data_timeout)

        # with database CGI-param for database_allowed_sids level
        _assert_status(base_url, _topic_data_path(ep, database=True), token, 200, timeout=topic_data_timeout)

        # check with and without database CGI-params for different access levels
        token = 'viewer@builtin'
        _assert_status(base_url, _topic_data_path(ep, database=True), token, 200, timeout=topic_data_timeout)
        _assert_status(base_url, _topic_data_path(ep, database=False), token, 200, timeout=topic_data_timeout)
        token = 'monitoring@builtin'
        _assert_status(base_url, _topic_data_path(ep, database=True), token, 200, timeout=topic_data_timeout)
        _assert_status(base_url, _topic_data_path(ep, database=False), token, 200, timeout=topic_data_timeout)
        token = 'root@builtin'
        _assert_status(base_url, _topic_data_path(ep, database=True), token, 200, timeout=topic_data_timeout)
        _assert_status(base_url, _topic_data_path(ep, database=False), token, 200, timeout=topic_data_timeout)


# database@builtin is a strict database-only token and must be rejected when path is out of database scope.
def test_viewer_describe_out_of_scope_path(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    db_qs = DATABASE.replace("/", "%2F")
    for ep in ['/viewer/describe', '/viewer/json/describe']:
        path = f'{ep}?database={db_qs}&path=%2FOther'
        _assert_status(base_url, path, 'database@builtin', 400)


# Only params that bypass regular path validation (e.g. path_id, schemeshard_id) are forbidden.
def test_viewer_describe_strict_database_token_extra_params(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    db_qs = DATABASE.replace("/", "%2F")
    for ep in ['/viewer/describe', '/viewer/json/describe']:
        path = f'{ep}?database={db_qs}&merge=true'
        _assert_not_status(base_url, path, 'database@builtin', 403)
        _assert_not_status(base_url, path, 'root@builtin', 403)


# path_id bypasses regular path validation, so handler validation rejects it as a bad request.
def test_viewer_describe_strict_database_token_forbidden_params(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    db_qs = DATABASE.replace("/", "%2F")
    for ep in ['/viewer/describe', '/viewer/json/describe']:
        path = f'{ep}?database={db_qs}&path_id=1'
        _assert_status(base_url, path, 'database@builtin', 400)
        # Non-database-scoped tokens must not be blocked by this check.
        _assert_not_status(base_url, path, 'root@builtin', 403)


# Path outside database scope gives endpoint validation error (400), not role-denied (403).
def test_out_of_scope_path_nodes_gives_400(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    db_qs = DATABASE.replace("/", "%2F")

    _run_query(base_url, f"REVOKE ALL ON `{DATABASE}` FROM `database@builtin`;")

    for ep in ['/viewer/nodes', '/viewer/json/nodes']:
        path = f'{ep}?database={db_qs}&path=%2FOther'
        _assert_status(base_url, path, 'database@builtin', 400)
        _assert_not_status(base_url, path, 'database@builtin', 403)


# schemeshard_id bypasses regular path validation, so handler validation rejects it as a bad request.
def test_viewer_describe_schemeshard_id_forbidden(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    db_qs = DATABASE.replace("/", "%2F")
    for ep in ['/viewer/describe', '/viewer/json/describe']:
        path = f'{ep}?database={db_qs}&schemeshard_id=1'
        _assert_status(base_url, path, 'database@builtin', 400)
        # Non-database-scoped tokens must not be blocked by this check.
        _assert_not_status(base_url, path, 'root@builtin', 403)
