# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor
import json

import requests
import yatest.common


requests.packages.urllib3.disable_warnings()

TOKENS = [
    None,
    'user@builtin',
    'database@builtin',
    'viewer@builtin',
    'monitoring@builtin',
    'root@builtin',
]

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


def _request_status(method, base_url, path, token):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    response = requests.request(method, base_url + path, headers=headers, verify=False, timeout=1)
    return response.status_code


def _endpoint_paths(endpoint_path, extra_query_strings=None):
    db_qs = f'?database={DATABASE.replace("/", "%2F")}'
    return [endpoint_path + qs for qs in ['', db_qs] + (extra_query_strings or [])]


def _collect_paths(cluster, paths):
    node = cluster.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    requests_to_run = [
        (path, method, token)
        for path in paths
        for method in ('GET', 'POST')
        for token in TOKENS
    ]
    results = {path: {'GET': {}, 'POST': {}} for path in paths}
    with ThreadPoolExecutor(max_workers=64) as executor:
        futures = [
            executor.submit(_request_status, method, base_url, path, token)
            for path, method, token in requests_to_run
        ]
        for (path, method, token), future in zip(requests_to_run, futures):
            label = token if token is not None else '__none__'
            results[path][method][label] = future.result()
    return results


def _collect_results(cluster, endpoint_path, extra_query_strings=None):
    return _collect_paths(cluster, _endpoint_paths(endpoint_path, extra_query_strings))


def _collect_many_results(cluster, endpoint_specs):
    paths = []
    for endpoint_path, extra_query_strings in endpoint_specs:
        paths.extend(_endpoint_paths(endpoint_path, extra_query_strings))
    return _collect_paths(cluster, paths)


def _canonize(name, results):
    out_path = yatest.common.output_path(f'{name}.json')
    with open(out_path, 'w') as f:
        json.dump(results, f, indent=2)
        f.write('\n')
    return yatest.common.canonical_file(out_path, local=True, universal_lines=True)


def _assert_status(base_url, path, token, status):
    assert _get_status(base_url, path, token) == status
    assert _post_status(base_url, path, token) == status


def _assert_not_status(base_url, path, token, status):
    assert _get_status(base_url, path, token) != status
    assert _post_status(base_url, path, token) != status


# This snapshot intentionally verifies the exact observable HTTP status matrix,
# including both access-control decisions and endpoint-specific handler validation.
def _collect_all_endpoints(cluster):
    db_qs = DATABASE.replace("/", "%2F")
    return _collect_many_results(cluster, [
        ('/viewer/autocomplete', None),
        ('/viewer/bscontrollerinfo', None),
        ('/viewer/bsgroupinfo', None),
        ('/viewer/capabilities', None),
        ('/viewer/cluster', None),
        ('/viewer/compute', [f'?database={db_qs}&path={db_qs}']),
        ('/viewer/counters', None),
        ('/viewer/feature_flags', None),
        ('/viewer/graph', None),
        ('/viewer/groups', None),
        ('/viewer/hiveinfo', None),
        ('/viewer/hivestats', None),
        ('/viewer/multipart_counter', None),
        ('/viewer/simple_counter', None),
        ('/viewer/sse_counter', None),
        ('/viewer/sysinfo', None),
        ('/viewer/json/autocomplete', None),
        ('/viewer/json/bscontrollerinfo', None),
        ('/viewer/json/bsgroupinfo', None),
        ('/viewer/json/cluster', None),
        ('/viewer/json/compute', [f'?database={db_qs}&path={db_qs}']),
        ('/viewer/json/counters', None),
        ('/viewer/json/feature_flags', None),
        ('/viewer/json/graph', None),
        ('/viewer/json/groups', None),
        ('/viewer/json/hiveinfo', None),
        ('/viewer/json/hivestats', None),
        ('/viewer/json/multipart_counter', None),
        ('/viewer/json/netinfo', [f'?database={db_qs}&path={db_qs}']),
        ('/viewer/json/nodeinfo', None),
        ('/viewer/json/nodelist', None),
        ('/viewer/json/nodes', None),
        ('/viewer/json/pdiskinfo', None),
        ('/viewer/json/pqconsumerinfo', [f'?database={db_qs}&topic={db_qs}%2Ftest-topic']),
        ('/viewer/json/render', None),
        ('/viewer/json/simple_counter', None),
        ('/viewer/json/sse_counter', None),
        ('/viewer/json/storage', None),
        ('/viewer/json/storage_usage', [f'?database={db_qs}&tenant={db_qs}']),
        ('/viewer/json/tenants', None),
        ('/viewer/json/topic_data', None),
        ('/viewer/json/topicinfo', [f'?database={db_qs}&path={db_qs}']),
        ('/viewer/json/vdiskinfo', None),
        ('/viewer/netinfo', [f'?database={db_qs}&path={db_qs}']),
        ('/viewer/nodeinfo', None),
        ('/viewer/nodelist', None),
        ('/viewer/nodes', None),
        ('/viewer/pdiskinfo', None),
        ('/viewer/peers', None),
        ('/viewer/pqconsumerinfo', [f'?database={db_qs}&topic={db_qs}%2Ftest-topic']),
        ('/viewer/render', None),
        ('/storage/groups', None),
        ('/viewer/storage', None),
        ('/viewer/json/tabletinfo', None),
        ('/viewer/tabletinfo', None),
        ('/viewer/storage_usage', [f'?database={db_qs}&tenant={db_qs}']),
        ('/viewer/tenants', None),
        ('/viewer/topic_data', None),
        ('/viewer/topicinfo', [f'?database={db_qs}&path={db_qs}']),
        ('/viewer/vdiskinfo', None),
        ('/viewer/v2/json/config', None),
        ('/viewer/v2/json/sysinfo', None),
        ('/viewer/v2/json/pdiskinfo', None),
        ('/viewer/v2/json/vdiskinfo', None),
        ('/viewer/v2/json/storage', None),
        ('/viewer/v2/json/nodelist', None),
        ('/viewer/v2/json/tabletinfo', None),
        ('/viewer/v2/json/nodeinfo', None),
    ])


def test_viewer_access_controls(ydb_cluster_with_external_access_controls):
    return _canonize(
        'viewer_access_controls',
        _collect_all_endpoints(ydb_cluster_with_external_access_controls),
    )


def test_viewer_access_controls_with_config_sids_flag(ydb_cluster_with_config_sids_flag):
    results = _collect_many_results(ydb_cluster_with_config_sids_flag, [
        ('/viewer/config', None),
        ('/viewer/json/config', None),
        ('/viewer/json/sysinfo', None),
    ])

    node = ydb_cluster_with_config_sids_flag.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'

    # When enable_viewer_allowed_sids_for_config_and_legacy_sysinfo is set, /viewer/config,
    # /viewer/json/config and /viewer/json/sysinfo become viewer-level endpoints:
    for ep in ['/viewer/config', '/viewer/json/config', '/viewer/json/sysinfo']:
        _assert_status(base_url, ep, 'database@builtin', 403)
        _assert_not_status(base_url, ep, 'viewer@builtin', 403)
        _assert_not_status(base_url, ep, 'monitoring@builtin', 403)
        _assert_not_status(base_url, ep, 'root@builtin', 403)

    return _canonize(
        'viewer_access_controls_config_sids',
        results,
    )


def test_viewer_v2_aliases_access_controls(ydb_cluster_with_config_sids_flag):
    node = ydb_cluster_with_config_sids_flag.nodes[1]
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


def test_viewer_describe_out_of_scope_path(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    db_qs = DATABASE.replace("/", "%2F")
    path = f'/viewer/describe?database={db_qs}&path=%2FOther'
    # database@builtin is a strict database-only token and must be rejected when path is out of database scope
    _assert_status(base_url, path, 'database@builtin', 400)


def test_viewer_json_describe_out_of_scope_path(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    db_qs = DATABASE.replace("/", "%2F")
    path = f'/viewer/json/describe?database={db_qs}&path=%2FOther'
    # database@builtin is a strict database-only token and must be rejected when path is out of database scope
    _assert_status(base_url, path, 'database@builtin', 400)


def test_viewer_describe_strict_database_token_extra_params(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    db_qs = DATABASE.replace("/", "%2F")
    path = f'/viewer/describe?database={db_qs}&merge=true'
    # Only params that bypass regular path validation (e.g. path_id, schemeshard_id) are forbidden.
    _assert_not_status(base_url, path, 'database@builtin', 403)
    _assert_not_status(base_url, path, 'root@builtin', 403)


def test_viewer_json_describe_strict_database_token_extra_params(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    db_qs = DATABASE.replace("/", "%2F")
    path = f'/viewer/json/describe?database={db_qs}&merge=true'
    # Only params that bypass regular path validation (e.g. path_id, schemeshard_id) are forbidden.
    _assert_not_status(base_url, path, 'database@builtin', 403)
    _assert_not_status(base_url, path, 'root@builtin', 403)


def test_viewer_describe_strict_database_token_forbidden_params(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    db_qs = DATABASE.replace("/", "%2F")
    path = f'/viewer/describe?database={db_qs}&path_id=1'
    # path_id bypasses regular path validation, so handler validation rejects it as a bad request.
    _assert_status(base_url, path, 'database@builtin', 400)
    # Non-database-scoped tokens must not be blocked by this check.
    _assert_not_status(base_url, path, 'root@builtin', 403)


def test_viewer_json_describe_strict_database_token_forbidden_params(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    db_qs = DATABASE.replace("/", "%2F")
    path = f'/viewer/json/describe?database={db_qs}&path_id=1'
    # path_id bypasses regular path validation, so handler validation rejects it as a bad request.
    _assert_status(base_url, path, 'database@builtin', 400)
    # Non-database-scoped tokens must not be blocked by this check.
    _assert_not_status(base_url, path, 'root@builtin', 403)


def test_require_database_nodelist_missing_gives_403(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    # database@builtin without ?database= must get 403 (RoleDenied), not 400
    _assert_status(base_url, '/viewer/nodelist', 'database@builtin', 403)
    _assert_status(base_url, '/viewer/json/nodelist', 'database@builtin', 403)


def test_require_database_tabletinfo_missing_gives_403(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    # database@builtin without ?database= must get 403 (RoleDenied), not 400
    _assert_status(base_url, '/viewer/tabletinfo', 'database@builtin', 403)
    _assert_status(base_url, '/viewer/json/tabletinfo', 'database@builtin', 403)


def test_require_database_autocomplete_missing_gives_403(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    # database@builtin without ?database= must get 403 (RoleDenied), not 400
    _assert_status(base_url, '/viewer/autocomplete', 'database@builtin', 403)
    _assert_status(base_url, '/viewer/json/autocomplete', 'database@builtin', 403)


# database@builtin without required parameters must get 403 (RoleDenied), not 400.

def test_require_database_nodes_missing_gives_403(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    _assert_status(base_url, '/viewer/nodes', 'database@builtin', 403)
    _assert_status(base_url, '/viewer/json/nodes', 'database@builtin', 403)


def test_describe_missing_path_or_database_gives_403_for_database_token(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    # database@builtin without path AND database must get 403 (RoleDenied), not 400
    _assert_status(base_url, '/viewer/describe', 'database@builtin', 403)
    _assert_status(base_url, '/viewer/json/describe', 'database@builtin', 403)


def test_out_of_scope_path_nodes_gives_400(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    db_qs = DATABASE.replace("/", "%2F")
    # path outside database scope → endpoint validation error (400), not role-denied (403)
    for ep in ['/viewer/nodes', '/viewer/json/nodes']:
        path = f'{ep}?database={db_qs}&path=%2FOther'
        _assert_status(base_url, path, 'database@builtin', 400)
        _assert_not_status(base_url, path, 'database@builtin', 403)


def test_viewer_describe_schemeshard_id_forbidden(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    db_qs = DATABASE.replace("/", "%2F")
    # schemeshard_id bypasses regular path validation, so handler validation rejects it as a bad request.
    for ep in ['/viewer/describe', '/viewer/json/describe']:
        path = f'{ep}?database={db_qs}&schemeshard_id=1'
        _assert_status(base_url, path, 'database@builtin', 400)
        # Non-database-scoped tokens must not be blocked by this check.
        _assert_not_status(base_url, path, 'root@builtin', 403)
