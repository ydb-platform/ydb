# -*- coding: utf-8 -*-
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
    response = requests.get(base_url + path, headers=headers, verify=False, timeout=10)
    return response.status_code


def _post_status(base_url, path, token):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    response = requests.post(base_url + path, headers=headers, verify=False, timeout=10)
    return response.status_code


def _collect_results(cluster, endpoint_path, extra_query_strings=None):
    node = cluster.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    db_qs = f'?database={DATABASE.replace("/", "%2F")}'
    query_strings = ['', db_qs] + (extra_query_strings or [])
    results = {}
    for qs in query_strings:
        full_path = endpoint_path + qs
        get_qs_results = {}
        post_qs_results = {}
        for token in TOKENS:
            label = token if token is not None else '__none__'
            get_qs_results[label] = _get_status(base_url, full_path, token)
            post_qs_results[label] = _post_status(base_url, full_path, token)
        results[full_path] = {'GET': get_qs_results, 'POST': post_qs_results}
    return results


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
    results = {}
    results.update(_collect_results(cluster, '/viewer/autocomplete'))
    results.update(_collect_results(cluster, '/viewer/bscontrollerinfo'))
    results.update(_collect_results(cluster, '/viewer/bsgroupinfo'))
    results.update(_collect_results(cluster, '/viewer/capabilities'))
    results.update(_collect_results(cluster, '/viewer/cluster'))
    results.update(_collect_results(cluster, '/viewer/compute', [f'?database={db_qs}&path={db_qs}']))
    results.update(_collect_results(cluster, '/viewer/counters'))
    results.update(_collect_results(cluster, '/viewer/feature_flags'))
    results.update(_collect_results(cluster, '/viewer/graph'))
    results.update(_collect_results(cluster, '/viewer/groups'))
    results.update(_collect_results(cluster, '/viewer/hiveinfo'))
    results.update(_collect_results(cluster, '/viewer/hivestats'))
    results.update(_collect_results(cluster, '/viewer/multipart_counter'))
    results.update(_collect_results(cluster, '/viewer/simple_counter'))
    results.update(_collect_results(cluster, '/viewer/sse_counter'))
    results.update(_collect_results(cluster, '/viewer/sysinfo'))
    results.update(_collect_results(cluster, '/viewer/json/autocomplete'))
    results.update(_collect_results(cluster, '/viewer/json/bscontrollerinfo'))
    results.update(_collect_results(cluster, '/viewer/json/bsgroupinfo'))
    results.update(_collect_results(cluster, '/viewer/json/cluster'))
    results.update(_collect_results(cluster, '/viewer/json/compute', [f'?database={db_qs}&path={db_qs}']))
    results.update(_collect_results(cluster, '/viewer/json/counters'))
    results.update(_collect_results(cluster, '/viewer/json/feature_flags'))
    results.update(_collect_results(cluster, '/viewer/json/graph'))
    results.update(_collect_results(cluster, '/viewer/json/groups'))
    results.update(_collect_results(cluster, '/viewer/json/hiveinfo'))
    results.update(_collect_results(cluster, '/viewer/json/hivestats'))
    results.update(_collect_results(cluster, '/viewer/json/multipart_counter'))
    results.update(_collect_results(cluster, '/viewer/json/netinfo', [f'?database={db_qs}&path={db_qs}']))
    results.update(_collect_results(cluster, '/viewer/json/nodeinfo'))
    results.update(_collect_results(cluster, '/viewer/json/nodelist'))
    results.update(_collect_results(cluster, '/viewer/json/nodes'))
    results.update(_collect_results(cluster, '/viewer/json/pdiskinfo'))
    results.update(_collect_results(cluster, '/viewer/json/pqconsumerinfo', [f'?database={db_qs}&topic={db_qs}%2Ftest-topic']))
    results.update(_collect_results(cluster, '/viewer/json/render'))
    results.update(_collect_results(cluster, '/viewer/json/simple_counter'))
    results.update(_collect_results(cluster, '/viewer/json/sse_counter'))
    results.update(_collect_results(cluster, '/viewer/json/storage'))
    results.update(_collect_results(cluster, '/viewer/json/storage_usage', [f'?database={db_qs}&tenant={db_qs}']))
    results.update(_collect_results(cluster, '/viewer/json/tenants'))
    results.update(_collect_results(cluster, '/viewer/json/topic_data'))
    results.update(_collect_results(cluster, '/viewer/json/topicinfo', [f'?database={db_qs}&path={db_qs}']))
    results.update(_collect_results(cluster, '/viewer/json/vdiskinfo'))
    results.update(_collect_results(cluster, '/viewer/netinfo', [f'?database={db_qs}&path={db_qs}']))
    results.update(_collect_results(cluster, '/viewer/nodeinfo'))
    results.update(_collect_results(cluster, '/viewer/nodelist'))
    results.update(_collect_results(cluster, '/viewer/nodes'))
    results.update(_collect_results(cluster, '/viewer/pdiskinfo'))
    results.update(_collect_results(cluster, '/viewer/peers'))
    results.update(_collect_results(cluster, '/viewer/pqconsumerinfo', [f'?database={db_qs}&topic={db_qs}%2Ftest-topic']))
    results.update(_collect_results(cluster, '/viewer/render'))
    results.update(_collect_results(cluster, '/storage/groups'))
    results.update(_collect_results(cluster, '/storage/groups/'))
    results.update(_collect_results(cluster, '/viewer/storage'))
    results.update(_collect_results(cluster, '/viewer/json/tabletinfo'))
    results.update(_collect_results(cluster, '/viewer/tabletinfo'))
    results.update(_collect_results(cluster, '/viewer/tabletinfo/'))
    results.update(_collect_results(cluster, '/viewer/storage_usage', [f'?database={db_qs}&tenant={db_qs}']))
    results.update(_collect_results(cluster, '/viewer/tenants'))
    results.update(_collect_results(cluster, '/viewer/topic_data'))
    results.update(_collect_results(cluster, '/viewer/topicinfo', [f'?database={db_qs}&path={db_qs}']))
    results.update(_collect_results(cluster, '/viewer/vdiskinfo'))
    results.update(_collect_results(cluster, '/viewer/v2/json/config'))
    results.update(_collect_results(cluster, '/viewer/v2/json/sysinfo'))
    results.update(_collect_results(cluster, '/viewer/v2/json/pdiskinfo'))
    results.update(_collect_results(cluster, '/viewer/v2/json/vdiskinfo'))
    results.update(_collect_results(cluster, '/viewer/v2/json/storage'))
    results.update(_collect_results(cluster, '/viewer/v2/json/nodelist'))
    results.update(_collect_results(cluster, '/viewer/v2/json/tabletinfo'))
    results.update(_collect_results(cluster, '/viewer/v2/json/nodeinfo'))
    return results


def test_viewer_access_controls(ydb_cluster_with_external_access_controls):
    return _canonize(
        'viewer_access_controls',
        _collect_all_endpoints(ydb_cluster_with_external_access_controls),
    )


def test_viewer_access_controls_with_config_sids_flag(ydb_cluster_with_config_sids_flag):
    results = {}
    results.update(_collect_results(ydb_cluster_with_config_sids_flag, '/viewer/config'))
    results.update(_collect_results(ydb_cluster_with_config_sids_flag, '/viewer/json/config'))
    results.update(_collect_results(ydb_cluster_with_config_sids_flag, '/viewer/json/sysinfo'))

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
        _assert_not_status(base_url, ep, 'viewer@builtin', 403)
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
    # path_id bypasses regular path validation and must be forbidden (403) for database-scoped tokens.
    _assert_status(base_url, path, 'database@builtin', 403)
    # Non-database-scoped tokens must not be blocked by this check.
    _assert_not_status(base_url, path, 'root@builtin', 403)


def test_viewer_json_describe_strict_database_token_forbidden_params(ydb_cluster_with_external_access_controls):
    node = ydb_cluster_with_external_access_controls.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    db_qs = DATABASE.replace("/", "%2F")
    path = f'/viewer/json/describe?database={db_qs}&path_id=1'
    # path_id bypasses regular path validation and must be forbidden (403) for database-scoped tokens.
    _assert_status(base_url, path, 'database@builtin', 403)
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
    _assert_status(base_url, '/viewer/tabletinfo/', 'database@builtin', 403)
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
    # schemeshard_id bypasses regular path validation and must be forbidden (403) for database-scoped tokens.
    for ep in ['/viewer/describe', '/viewer/json/describe']:
        path = f'{ep}?database={db_qs}&schemeshard_id=1'
        _assert_status(base_url, path, 'database@builtin', 403)
        # Non-database-scoped tokens must not be blocked by this check.
        _assert_not_status(base_url, path, 'root@builtin', 403)
