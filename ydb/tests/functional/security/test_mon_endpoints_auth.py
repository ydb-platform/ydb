# -*- coding: utf-8 -*-
import pytest
import requests


EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN = {
    '/counters': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/counters/hosts': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/healthcheck?format=prometheus': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/healthcheck': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/healthcheck?database=%2FRoot': {
        None: 200,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/ping': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/status': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/ver': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/login': {
        None: 400,
        'user@builtin': 400,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/capabilities': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/static/css/bootstrap.min.css': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/monitoring/': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/internal': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
}

EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN = {
    '/counters': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/counters/hosts': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/healthcheck?format=prometheus': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/healthcheck': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/healthcheck?database=%2FRoot': {
        None: 200,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/ping': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/status': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/ver': {
        None: 200,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/login': {
        None: 400,
        'user@builtin': 400,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/capabilities': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/static/css/bootstrap.min.css': {
        None: 200,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/monitoring/': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/internal': {
        None: 200,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/': {
        None: 200,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
}


assert len(EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN) == len(
    EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN
), "Handlers list must be the same"


def _test_endpoint(endpoint_url, endpoint_path, token, expected_status):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    response = requests.get(endpoint_url, headers=headers, verify=False)
    token_desc = token if token is not None else "null"
    assert (
        response.status_code == expected_status
    ), f"Expected {endpoint_path} with token={token_desc} to return {expected_status}, got {response.status_code}"


def _test_endpoints_via_node_proxy(cluster, node_index, path_suffix, expected_statuses_by_token):
    node = cluster.nodes[node_index]
    base_url = f'https://{node.host}:{node.mon_port}'
    node_id = node.node_id
    full_path = f'/node/{node_id}{path_suffix}'
    endpoint_url = f'{base_url}{full_path}'
    for token, expected_status in expected_statuses_by_token.items():
        _test_endpoint(endpoint_url, full_path, token, expected_status)


def _test_endpoints(cluster, expected_results):
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    base_url = f'https://{host}:{mon_port}'

    for endpoint_path, expected_statuses in expected_results.items():
        endpoint_url = f'{base_url}{endpoint_path}'
        for token, expected_status in expected_statuses.items():
            _test_endpoint(endpoint_url, endpoint_path, token, expected_status)


def _test_post_json_endpoint(base_url, endpoint_path, token, expected_status, json_body=None):
    headers = {'Content-Type': 'application/json'}
    if token is not None:
        headers['Authorization'] = token
    body = {} if json_body is None else json_body
    response = requests.post(
        f'{base_url}{endpoint_path}',
        headers=headers,
        json=body,
        verify=False,
    )
    token_desc = token if token is not None else 'null'
    assert response.status_code == expected_status, (
        f'POST {endpoint_path!r} token={token_desc!r}: expected {expected_status}, '
        f'got {response.status_code}: {response.text[:500]}'
    )


def test_with_enforce_user_token(ydb_cluster_with_enforce_user_token):
    _test_endpoints(ydb_cluster_with_enforce_user_token, EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN)


def test_without_enforce_user_token(ydb_cluster_without_enforce_user_token):
    _test_endpoints(ydb_cluster_without_enforce_user_token, EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN)


def test_with_require_counters_authentication(ydb_cluster_with_require_counters_auth):
    EXPECTED_RESULTS_WITH_REQUIRE_COUNTERS_AUTH = {
        '/counters': {
            None: 401,
            'user@builtin': 403,
            'database@builtin': 403,
            'viewer@builtin': 200,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        },
        '/counters/hosts': {
            None: 401,
            'user@builtin': 403,
            'database@builtin': 403,
            'viewer@builtin': 200,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        },
        '/ping': {
            None: 200,
            'user@builtin': 200,
            'database@builtin': 200,
            'viewer@builtin': 200,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        },  # checks this just in case
    }
    _test_endpoints(ydb_cluster_with_require_counters_auth, EXPECTED_RESULTS_WITH_REQUIRE_COUNTERS_AUTH)


def test_with_require_healthcheck_authentication(ydb_cluster_with_require_healthcheck_auth):
    EXPECTED_RESULTS_WITH_REQUIRE_HEALTHCHECK_AUTH = {
        '/healthcheck?format=prometheus': {
            None: 401,
            'user@builtin': 403,
            'database@builtin': 403,
            'viewer@builtin': 200,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        },
        '/healthcheck': {
            None: 401,
            'user@builtin': 403,
            'database@builtin': 403,
            'viewer@builtin': 403,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        },
        '/healthcheck?database=%2FRoot': {
            None: 401,
            'user@builtin': 403,
            'database@builtin': 403,
            'viewer@builtin': 403,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        },
        '/healthcheck?database=%2FRoot&format=prometheus': {
            None: 401,
            'user@builtin': 403,
            'database@builtin': 403,
            'viewer@builtin': 200,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        },
        '/ping': {
            None: 200,
            'user@builtin': 200,
            'database@builtin': 200,
            'viewer@builtin': 200,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        },  # checks this just in case
    }
    _test_endpoints(ydb_cluster_with_require_healthcheck_auth, EXPECTED_RESULTS_WITH_REQUIRE_HEALTHCHECK_AUTH)


PUBLIC_ENDPOINTS_LIST = [
    '/actors/tablet_counters_aggregator',
    '/counters',
    '/counters/hosts',
    '/followercounters',
    '/labeledcounters',
    '/ping',
    '/status',
    '/viewer/capabilities',
    '/monitoring/',  # built-in authorization page, so it returns 200
]


def test_public_endpoints_list_with_enforce_user_token(ydb_cluster_with_enforce_user_token):
    expected_results = {
        endpoint_path: {
            None: 200,
            'user@builtin': 200,
            'database@builtin': 200,
            'viewer@builtin': 200,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        }
        for endpoint_path in PUBLIC_ENDPOINTS_LIST
    }
    _test_endpoints(ydb_cluster_with_enforce_user_token, expected_results)


# These endpoints downgrade access level to public when specific parameters are provided.
def test_public_endpoints_with_params_with_enforce_user_token(ydb_cluster_with_enforce_user_token):
    _test_endpoints(
        ydb_cluster_with_enforce_user_token,
        {
            # Enabled feature flag `require_healthcheck_authentication` forbids public access.
            '/healthcheck?format=prometheus': {
                None: 200,
                'user@builtin': 200,
                'database@builtin': 200,
                'viewer@builtin': 200,
                'monitoring@builtin': 200,
                'root@builtin': 200,
            },
            # Enabled feature flag `require_healthcheck_authentication` forbids public access.
            '/healthcheck?database=%2FRoot': {
                None: 200,
                'user@builtin': 403,
                'database@builtin': 403,
                'viewer@builtin': 403,
                'monitoring@builtin': 200,
                'root@builtin': 200,
            },
        },
    )


# Built-in authorization page, so it returns 200
@pytest.mark.parametrize('node_index', [1])
def test_node_proxy_monitoring_builtin_auth_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token,
    node_index,
):
    all_ok = {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    }
    _test_endpoints_via_node_proxy(
        ydb_cluster_with_enforce_user_token,
        node_index,
        '/monitoring',
        all_ok,
    )


OPERATOR_ENDPOINTS_LIST = [
    '/actors/',
    '/actors/configs_dispatcher',
    '/actors/console_configs_provider',
    '/actors/dnameserver',
    '/actors/dsproxynode',
    '/actors/feature_flags',
    '/actors/icb',
    '/actors/kqp_node',
    '/actors/kqp_proxy',
    '/actors/kqp_resource_manager',
    '/actors/kqp_spilling_file',
    '/actors/logger',
    '/actors/memory_tracker',
    '/actors/netclassifier',
    '/actors/nodewarden',
    '/actors/pql2',
    '/actors/quoter_proxy',
    '/actors/rb',
    '/actors/statservice',
    '/actors/tenant_pool',
    '/cms',
    '/grpc',
    '/healthcheck',
    '/internal',
    '/jquery.tablesorter.css',
    '/jquery.tablesorter.js',
    '/memory/fragmentation',
    '/memory/heap',
    '/memory/peakheap',
    '/memory/statistics',
    '/nodetabmon',
    '/static/css/bootstrap.min.css',
    '/static/fonts/glyphicons-halflings-regular.eot',
    '/static/fonts/glyphicons-halflings-regular.svg',
    '/static/fonts/glyphicons-halflings-regular.ttf',
    '/static/fonts/glyphicons-halflings-regular.woff',
    '/static/js/bootstrap.min.js',
    '/static/js/jquery.min.js',
    '/tablet',
    '/tablets',
    '/trace',
    '/ver',
    '/viewer/cluster',
    '/viewer/config',
    '/viewer/healthcheck',
    '/viewer/v2/json/config',
    '/viewer/v2/json/nodelist',
    '/viewer/v2/json/storage',
]


OPERATOR_CONTEXT_DEPENDENT_GET_PATHS = [
    '/pdisk/info',
    '/vdisk/vdiskstat',
    '/vdisk/blobindexstat',
]


OPERATOR_UNRESOLVED_ENDPOINTS_LIST = [
    '/actors/lease',
    '/actors/row_dispatcher',
    '/actors/schemeboard',
    '/actors/sqsgc',
    '/actors/yq_control_plane_proxy',
    '/actors/yq_health',
    '/fq_diag/fetcher',
    '/fq_diag/local_worker_manager',
    '/fq_diag/quotas',
]


OPERATOR_POST_PATHS = [
    '/pdisk/restart',
    '/pdisk/status',
    '/vdisk/evict',
]


def test_operator_post_bs_controller_endpoints_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token,
):
    host = ydb_cluster_with_enforce_user_token.nodes[1].host
    mon_port = ydb_cluster_with_enforce_user_token.nodes[1].mon_port
    base_url = f'https://{host}:{mon_port}'
    expected_by_token = {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    }
    for endpoint_path in OPERATOR_POST_PATHS:
        for token, expected_status in expected_by_token.items():
            _test_post_json_endpoint(base_url, endpoint_path, token, expected_status, json_body={})


def test_operator_endpoints_list_with_enforce_user_token(ydb_cluster_with_enforce_user_token):
    expected_results = {
        endpoint_path: {
            None: 401,
            'user@builtin': 403,
            'database@builtin': 403,
            'viewer@builtin': 403,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        }
        for endpoint_path in OPERATOR_ENDPOINTS_LIST
    }
    _test_endpoints(ydb_cluster_with_enforce_user_token, expected_results)


def test_operator_context_dependent_get_endpoints_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token,
):
    expected_by_token = {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    }
    expected_results = {
        endpoint_path: expected_by_token for endpoint_path in OPERATOR_CONTEXT_DEPENDENT_GET_PATHS
    }
    _test_endpoints(ydb_cluster_with_enforce_user_token, expected_results)


def test_operator_unresolved_endpoints_list_with_enforce_user_token(ydb_cluster_with_enforce_user_token):
    expected_results = {
        endpoint_path: {
            None: 401,
            'user@builtin': 403,
            'database@builtin': 403,
            'viewer@builtin': 403,
            'monitoring@builtin': 404,
            'root@builtin': 404,
        }
        for endpoint_path in OPERATOR_UNRESOLVED_ENDPOINTS_LIST
    }
    _test_endpoints(ydb_cluster_with_enforce_user_token, expected_results)


ADMIN_BASE_GET_PATHS = [
    '/viewer/bscontrollerinfo',
]


ADMIN_CONTEXT_DEPENDENT_GET_PATHS = [
    '/viewer/topic_data',
    '/vdisk/getblob',
]


ADMIN_POST_PATHS = [
    '/pdisk/restart?force=true',
    '/pdisk/status?force=true',
    '/vdisk/evict?force=true',
]


def test_admin_get_endpoints_list_with_enforce_user_token(ydb_cluster_with_enforce_user_token):
    expected_by_token = {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 403,
        'root@builtin': 200,
    }
    expected_results = {
        endpoint_path: expected_by_token for endpoint_path in ADMIN_BASE_GET_PATHS
    }
    _test_endpoints(ydb_cluster_with_enforce_user_token, expected_results)


def test_admin_context_dependent_get_endpoints_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token,
):
    expected_by_token = {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 403,
        'root@builtin': 400,
    }
    expected_results = {
        endpoint_path: expected_by_token for endpoint_path in ADMIN_CONTEXT_DEPENDENT_GET_PATHS
    }
    _test_endpoints(ydb_cluster_with_enforce_user_token, expected_results)


def test_admin_post_bs_controller_endpoints_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token,
):
    host = ydb_cluster_with_enforce_user_token.nodes[1].host
    mon_port = ydb_cluster_with_enforce_user_token.nodes[1].mon_port
    base_url = f'https://{host}:{mon_port}'
    expected_by_token = {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 403,
        'root@builtin': 400,
    }
    for endpoint_path in ADMIN_POST_PATHS:
        for token, expected_status in expected_by_token.items():
            _test_post_json_endpoint(base_url, endpoint_path, token, expected_status, json_body={})
