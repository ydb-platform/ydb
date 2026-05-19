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

    for ep in ['/viewer/config', '/viewer/json/config', '/viewer/json/sysinfo']:
        _assert_status(base_url, ep, 'database@builtin', 403)
        _assert_not_status(base_url, ep, 'viewer@builtin', 403)
        _assert_not_status(base_url, ep, 'monitoring@builtin', 403)
        _assert_not_status(base_url, ep, 'root@builtin', 403)


def test_viewer_v2_aliases_access_controls(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    db_qs = f'?database={DATABASE.replace("/", "%2F")}'

    for ep in ['/viewer/v2/json/config', '/viewer/v2/json/config' + db_qs]:
        _assert_status(base_url, ep, 'database@builtin', 403)
        _assert_status(base_url, ep, 'viewer@builtin', 403)
        _assert_not_status(base_url, ep, 'monitoring@builtin', 403)
        _assert_not_status(base_url, ep, 'root@builtin', 403)

    for ep in ['/viewer/v2/json/sysinfo', '/viewer/v2/json/sysinfo' + db_qs]:
        _assert_status(base_url, ep, 'database@builtin', 403)
        _assert_not_status(base_url, ep, 'viewer@builtin', 403)
        _assert_not_status(base_url, ep, 'monitoring@builtin', 403)
        _assert_not_status(base_url, ep, 'root@builtin', 403)


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
