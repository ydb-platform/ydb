# -*- coding: utf-8 -*-
import requests


EXCESSIVE_INFO_ENDPOINTS = [
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


def _collect_statuses(cluster, token, with_database):
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    base_url = f'https://{host}:{mon_port}'
    headers = {'Authorization': token}
    suffix = '?database=%2FRoot' if with_database else ''

    statuses = {}
    for endpoint in EXCESSIVE_INFO_ENDPOINTS:
        response = requests.get(
            f'{base_url}{endpoint}{suffix}',
            headers=headers,
            verify=False,
        )
        statuses[endpoint] = response.status_code
    return statuses


def test_excessive_info_endpoints_require_database_parameter_for_database_token(
    ydb_cluster_with_enforce_user_token,
):
    statuses_without_database = _collect_statuses(
        ydb_cluster_with_enforce_user_token,
        'database@builtin',
        with_database=False,
    )
    statuses_with_database = _collect_statuses(
        ydb_cluster_with_enforce_user_token,
        'database@builtin',
        with_database=True,
    )

    leaked_without_database = [
        endpoint for endpoint, status in statuses_without_database.items() if status == 200
    ]
    assert not leaked_without_database, (
        'database@builtin must not access excessive-info endpoints without '
        f'database parameter, but got 200 for: {leaked_without_database}'
    )

    denied_with_database = [
        endpoint for endpoint, status in statuses_with_database.items() if status != 200
    ]
    assert not denied_with_database, (
        'database@builtin should access excessive-info endpoints with '
        f'database parameter, but got non-200 for: {denied_with_database}'
    )


def test_viewer_graph_is_forbidden_for_database_token(
    ydb_cluster_with_enforce_user_token,
):
    host = ydb_cluster_with_enforce_user_token.nodes[1].host
    mon_port = ydb_cluster_with_enforce_user_token.nodes[1].mon_port
    base_url = f'https://{host}:{mon_port}'

    response = requests.get(
        f'{base_url}/viewer/graph?target=cpu',
        headers={'Authorization': 'database@builtin'},
        verify=False,
    )
    assert response.status_code == 403, response.text
