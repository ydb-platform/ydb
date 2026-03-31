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
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/healthcheck?database=%2FRoot': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
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
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
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
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
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
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
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


def _test_endpoints_with_payloads(cluster, endpoint_cases, method='POST'):
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
    '/viewer/plan2svg',
    # Endpoints below expose excessive information.
    '/viewer/bscontrollerinfo',
    '/viewer/cluster',
    '/viewer/compute',
    '/viewer/config',
    '/viewer/counters',
    '/viewer/hiveinfo',
    '/viewer/hivestats',
    '/viewer/labeledcounters',
    '/viewer/netinfo',
    '/viewer/pqconsumerinfo',
    '/viewer/storage',
    '/viewer/storage_usage',
    '/viewer/tenants',
    '/viewer/topicinfo',
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
    '/operation/get',
    '/operation/list',
    '/query/script/fetch',
    '/viewer/browse',
    '/viewer/describe_consumer',
    '/viewer/describe_replication',
    '/viewer/describe_topic',
    '/viewer/describe_transfer',
    '/viewer/graph',
    '/viewer/metainfo',
    '/viewer/topic_data',
    '/operation/list?database=%2FRoot',
    '/operation/forget',
    '/viewer/commit_offset',
    '/operation/cancel',
    '/query/script/execute',
    '/scheme/directory',
    '/viewer/put_record',
]

DATABASE_ENDPOINTS_REQUIRING_PARAMETERS_OR_REQUEST_CONTEXT_POST_METHOD_LIST = [
    '/operation/forget',
    '/viewer/commit_offset',
    '/operation/cancel',
    '/query/script/execute',
    '/viewer/put_record',
    '/viewer/query',
    '/viewer/acl',
]

DATABASE_ENDPOINTS_REQUIRING_PARAMETERS_OR_REQUEST_CONTEXT_GET_AND_POST_METHOD_LIST = [
    '/scheme/directory',
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
        if (
            endpoint_path not in DATABASE_ENDPOINTS_REQUIRING_PARAMETERS_OR_REQUEST_CONTEXT_POST_METHOD_LIST
            or endpoint_path in DATABASE_ENDPOINTS_REQUIRING_PARAMETERS_OR_REQUEST_CONTEXT_GET_AND_POST_METHOD_LIST
        )
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
        '/viewer/query?database=%2FRoot': {
            'expected_statuses': expected_statuses_for_invalid_request,
            'json_body': {},
        },
        '/viewer/acl?database=%2FRoot': {
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


@pytest.mark.xfail(reason='Empty request handling is unstable for this endpoint in the current environment')
def test_database_endpoints_with_unstable_empty_request_handling_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token,
):
    _test_endpoints(
        ydb_cluster_with_enforce_user_token,
        {
            '/viewer/content': {
                None: 401,
                'user@builtin': 403,
                'database@builtin': 504,
                'viewer@builtin': 504,
                'monitoring@builtin': 504,
                'root@builtin': 504,
            },
            '/viewer/tabletcounters': {
                None: 401,
                'user@builtin': 403,
                'database@builtin': None,
                'viewer@builtin': None,
                'monitoring@builtin': None,
                'root@builtin': None,
            },
        },
    )


VIEWER_ENDPOINTS_LIST = [
    '/storage/groups',
    '/viewer/bsgroupinfo',
    '/viewer/groups',
    '/viewer/multipart_counter',
    '/viewer/nodeinfo',
    '/viewer/nodes',
    '/viewer/pdiskinfo',
    '/viewer/peers',
    '/viewer/simple_counter',
    '/viewer/sse_counter',
    '/viewer/sysinfo',
    '/viewer/tabletinfo',
    '/viewer/v2/json/nodeinfo',
    '/viewer/v2/json/pdiskinfo',
    '/viewer/v2/json/sysinfo',
    '/viewer/v2/json/tabletinfo',
    '/viewer/v2/json/vdiskinfo',
    '/viewer/vdiskinfo',
    # Endpoints below expose excessive information or grant excessive rights.
    '/viewer/autocomplete',
    '/viewer/feature_flags',
    '/viewer/nodelist',
    '/viewer/tenantinfo',
    '/viewer/whoami',
]


def test_viewer_endpoints_list_with_enforce_user_token(ydb_cluster_with_enforce_user_token):
    expected_results = {
        endpoint_path: {
            None: 401,
            'user@builtin': 403,
            'database@builtin': 403,
            'viewer@builtin': 200,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        }
        for endpoint_path in VIEWER_ENDPOINTS_LIST
    }
    _test_endpoints(ydb_cluster_with_enforce_user_token, expected_results)


VIEWER_ENDPOINTS_REQUIRING_PARAMETERS_OR_REQUEST_CONTEXT_LIST = [
    '/viewer/render',
    # Endpoints below expose excessive information or grant excessive rights.
    '/pdisk/info',
    '/pdisk/restart',
    '/pdisk/status',
    '/vdisk/blobindexstat',
    '/vdisk/evict',
    '/vdisk/getblob',
    '/vdisk/vdiskstat',
    '/viewer/acl',
    '/viewer/check_access',
    '/viewer/database_stats',
    '/viewer/describe',
    '/viewer/hotkeys',
    '/viewer/query',
    '/viewer/storage_stats',
]


def test_viewer_endpoints_requiring_parameters_or_request_context_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token,
):
    expected_statuses_for_invalid_viewer_request = {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    }

    expected_results_get = {
        endpoint_path: expected_statuses_for_invalid_viewer_request
        for endpoint_path in VIEWER_ENDPOINTS_REQUIRING_PARAMETERS_OR_REQUEST_CONTEXT_LIST
        if endpoint_path not in OPERATOR_ENDPOINTS_REQUIRING_PARAMETERS_OR_REQUEST_CONTEXT_POST_METHOD_LIST
    }
    _test_endpoints(ydb_cluster_with_enforce_user_token, expected_results_get)


# POST endpoints below require monitoring/admin access level on method gate.
OPERATOR_ENDPOINTS_REQUIRING_PARAMETERS_OR_REQUEST_CONTEXT_POST_METHOD_LIST = [
    '/pdisk/restart',
    '/pdisk/status',
    '/vdisk/evict',
]


def test_operator_post_endpoints_requiring_parameters_or_request_context_with_enforce_user_token(
    ydb_cluster_with_enforce_user_token,
):
    expected_statuses_for_invalid_operator_request = {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    }

    expected_results_post_operator = {
        '/pdisk/restart?pdisk_id=invalid': {
            'expected_statuses': expected_statuses_for_invalid_operator_request,
            'json_body': {},
        },
        '/pdisk/status?pdisk_id=invalid': {
            'expected_statuses': expected_statuses_for_invalid_operator_request,
            'json_body': {},
        },
        '/vdisk/evict?vdisk_id=invalid': {
            'expected_statuses': expected_statuses_for_invalid_operator_request,
            'json_body': {},
        },
    }
    _test_endpoints_with_payloads(
        ydb_cluster_with_enforce_user_token,
        expected_results_post_operator,
        method='POST',
    )
