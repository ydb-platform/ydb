# -*- coding: utf-8 -*-
import requests

requests.packages.urllib3.disable_warnings()

DISABLED_AUTHENTICATION_PATHS = ['/ver', '/trace']


def _base_url(cluster):
    node = cluster.nodes[1]
    return f'https://{node.host}:{node.mon_port}'


def _get_status(base_url, path, token=None):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    response = requests.get(base_url + path, headers=headers, verify=False, timeout=5)
    return response.status_code


def _assert_status(base_url, path, token, status):
    actual = _get_status(base_url, path, token)
    token_desc = token if token is not None else '__none__'
    assert actual == status, (
        f'Expected {path} with token={token_desc} to return {status}, got {actual}'
    )


def test_exact_path_no_auth_required(ydb_cluster_with_disabled_authentication_paths):
    base_url = _base_url(ydb_cluster_with_disabled_authentication_paths)

    for path in DISABLED_AUTHENTICATION_PATHS:
        _assert_status(base_url, path, None, 200)


def test_exact_path_with_query_no_auth_required(ydb_cluster_with_disabled_authentication_paths):
    base_url = _base_url(ydb_cluster_with_disabled_authentication_paths)

    for path in DISABLED_AUTHENTICATION_PATHS:
        _assert_status(base_url, path + '?foo=bar', None, 200)


def test_prefix_subpath_still_requires_auth(ydb_cluster_with_disabled_authentication_paths):
    base_url = _base_url(ydb_cluster_with_disabled_authentication_paths)

    for path in DISABLED_AUTHENTICATION_PATHS:
        _assert_status(base_url, path + '/subpath', None, 401)


def test_suffix_path_still_requires_auth(ydb_cluster_with_disabled_authentication_paths):
    base_url = _base_url(ydb_cluster_with_disabled_authentication_paths)

    for path in DISABLED_AUTHENTICATION_PATHS:
        _assert_status(base_url, '/prefix' + path, None, 401)


def test_unlisted_path_still_requires_auth(ydb_cluster_with_disabled_authentication_paths):
    base_url = _base_url(ydb_cluster_with_disabled_authentication_paths)

    # /actors/ normally requires auth when enforce_user_token is enabled.
    _assert_status(base_url, '/actors/', None, 401)


def test_disabled_paths_still_work_with_token(ydb_cluster_with_disabled_authentication_paths):
    base_url = _base_url(ydb_cluster_with_disabled_authentication_paths)

    for path in DISABLED_AUTHENTICATION_PATHS:
        _assert_status(base_url, path, 'monitoring@builtin', 200)
        _assert_status(base_url, path, 'root@builtin', 200)
