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


def _test_endpoint(endpoint_url, endpoint_path, token, expected_status, method='GET', json_body=None):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    response = requests.request(
        method=method,
        url=endpoint_url,
        headers=headers,
        json=json_body,
        verify=False,
    )
    token_desc = token if token is not None else "null"
    assert (
        response.status_code == expected_status
    ), (
        f"Expected {method} {endpoint_path} with token={token_desc} to return "
        f"{expected_status}, got {response.status_code}"
    )


def _test_endpoints_via_node_proxy(cluster, node_index, path_suffix, expected_statuses_by_token):
    node = cluster.nodes[node_index]
    base_url = f'https://{node.host}:{node.mon_port}'
    node_id = node.node_id
    full_path = f'/node/{node_id}{path_suffix}'
    endpoint_url = f'{base_url}{full_path}'
    for token, expected_status in expected_statuses_by_token.items():
        _test_endpoint(endpoint_url, full_path, token, expected_status)


def _test_endpoints(cluster, expected_results, method='GET', json_body=None):
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    base_url = f'https://{host}:{mon_port}'

    for endpoint_path, expected_statuses in expected_results.items():
        endpoint_url = f'{base_url}{endpoint_path}'
        for token, expected_status in expected_statuses.items():
            _test_endpoint(endpoint_url, endpoint_path, token, expected_status, method=method, json_body=json_body)


def _test_endpoints_with_payloads(cluster, endpoint_cases, method):
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    base_url = f'https://{host}:{mon_port}'

    for endpoint_path, case in endpoint_cases.items():
        endpoint_url = f'{base_url}{endpoint_path}'
        expected_statuses = case['expected_statuses']
        json_body = case.get('json_body')
        for token, expected_status in expected_statuses.items():
            _test_endpoint(endpoint_url, endpoint_path, token, expected_status, method=method, json_body=json_body)


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


DATABASE_ENDPOINTS_LIST = [
    '/viewer/bscontrollerinfo?database=%2FRoot',
    '/viewer/cluster?database=%2FRoot',
    '/viewer/compute?database=%2FRoot',
    '/viewer/config?database=%2FRoot',
    '/viewer/counters?database=%2FRoot',
    '/viewer/hiveinfo?database=%2FRoot',
    '/viewer/hivestats?database=%2FRoot',
    '/viewer/labeledcounters?database=%2FRoot',
    '/viewer/netinfo?database=%2FRoot',
    '/viewer/plan2svg',
    '/viewer/pqconsumerinfo?database=%2FRoot',
    '/viewer/storage?database=%2FRoot',
    '/viewer/storage_usage?database=%2FRoot',
    '/viewer/tenants?database=%2FRoot',
    '/viewer/topicinfo?database=%2FRoot',
]


def test_database_endpoints_list_with_enforce_user_token(ydb_cluster_with_enforce_user_token):
    expected_results = {
        endpoint_path: {
            None: 401,
            'user@builtin': 403,
            'database@builtin': 200,
            'viewer@builtin': 200,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        }
        for endpoint_path in DATABASE_ENDPOINTS_LIST
    }
    _test_endpoints(ydb_cluster_with_enforce_user_token, expected_results)


DATABASE_ENDPOINTS_REQUIRING_PARAMETERS_OR_REQUEST_CONTEXT_LIST = [
    '/operation/cancel',
    '/operation/forget',
    '/operation/get',
    '/operation/list',
    '/operation/list?database=%2FRoot',
    '/query/script/execute',
    '/query/script/fetch',
    '/scheme/directory',
    '/viewer/browse?database=%2FRoot',
    '/viewer/commit_offset',
    '/viewer/content',
    '/viewer/describe_consumer',
    '/viewer/describe_replication',
    '/viewer/describe_topic',
    '/viewer/describe_transfer',
    '/viewer/graph?database=%2FRoot',
    '/viewer/metainfo?database=%2FRoot',
    '/viewer/tabletcounters',
]

DATABASE_ENDPOINTS_REQUIRING_PARAMETERS_OR_REQUEST_CONTEXT_POST_METHOD_LIST = [
    '/operation/cancel',
    '/operation/forget',
    '/query/script/execute',
    '/scheme/directory',
    '/viewer/commit_offset',
    '/viewer/put_record',
]


def test_database_endpoints_requiring_parameters_or_request_context_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token,
):
    expected_statuses_for_invalid_request = {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    }

    expected_results_get = {
        endpoint_path: expected_statuses_for_invalid_request
        for endpoint_path in DATABASE_ENDPOINTS_REQUIRING_PARAMETERS_OR_REQUEST_CONTEXT_LIST
        if endpoint_path not in DATABASE_ENDPOINTS_REQUIRING_PARAMETERS_OR_REQUEST_CONTEXT_POST_METHOD_LIST
    }
    _test_endpoints(ydb_cluster_with_enforce_user_token, expected_results_get)

    # POST checks include minimal parameters to pass method gate.
    expected_results_post = {
        '/operation/forget?database=%2FRoot&id=fake-op': {
            'expected_statuses': expected_statuses_for_invalid_request,
            'json_body': {},
        },
        '/viewer/commit_offset?database=%2FRoot&path=%2FRoot%2Fmissing-topic&consumer=c1&partition_id=0&offset=0': {
            'expected_statuses': expected_statuses_for_invalid_request,
            'json_body': {},
        },
        '/operation/cancel?database=%2FRoot&id=fake-op': {
            'expected_statuses': expected_statuses_for_invalid_request,
            'json_body': {},
        },
        '/query/script/execute?database=%2FRoot': {
            'expected_statuses': expected_statuses_for_invalid_request,
            'json_body': {},
        },
        '/scheme/directory?database=%2FRoot': {
            'expected_statuses': expected_statuses_for_invalid_request,
            'json_body': {},
        },
        '/viewer/put_record?database=%2FRoot': {
            'expected_statuses': expected_statuses_for_invalid_request,
            'json_body': {
                'path': '/Root/missing-topic',
                'message': 'm',
            },
        },
    }
    _test_endpoints_with_payloads(ydb_cluster_with_enforce_user_token, expected_results_post, method='POST')
