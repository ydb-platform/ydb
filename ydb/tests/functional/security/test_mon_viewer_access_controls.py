# -*- coding: utf-8 -*-
import requests

requests.packages.urllib3.disable_warnings()

DATABASE = '/Root'


def _get_status(base_url, path, token):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    response = requests.get(base_url + path, headers=headers, verify=False, timeout=1)
    return response.status_code


def _post_status(base_url, path, token):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    response = requests.post(base_url + path, headers=headers, verify=False, timeout=1)
    return response.status_code


def _assert_status(base_url, path, token, status):
    assert _get_status(base_url, path, token) == status
    assert _post_status(base_url, path, token) == status


def _assert_not_status(base_url, path, token, status):
    assert _get_status(base_url, path, token) != status
    assert _post_status(base_url, path, token) != status


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


def test_viewer_sysinfo_access_controls(ydb_cluster_with_external_access_controls):
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

    for ep in ['/viewer/sysinfo', '/viewer/json/sysinfo']:
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
