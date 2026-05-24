# -*- coding: utf-8 -*-
import json
import pytest
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

    statuses = {}
    for endpoint in EXCESSIVE_INFO_ENDPOINTS:
        endpoint_with_context = f'{endpoint}?database=%2FRoot' if with_database else endpoint
        response = requests.get(
            f'{base_url}{endpoint_with_context}',
            headers=headers,
            verify=False,
        )
        statuses[endpoint] = response.status_code
    return statuses


def _collect_responses(cluster, token, with_database):
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    base_url = f'https://{host}:{mon_port}'
    headers = {'Authorization': token}
    suffix = '?database=%2FRoot' if with_database else ''

    responses = {}
    for endpoint in EXCESSIVE_INFO_ENDPOINTS:
        response = requests.get(
            f'{base_url}{endpoint}{suffix}',
            headers=headers,
            verify=False,
        )
        responses[endpoint] = response
    return responses


def _json_shape(value, prefix=''):
    paths = set()
    if isinstance(value, dict):
        for key, nested_value in value.items():
            nested_prefix = f'{prefix}.{key}' if prefix else str(key)
            paths.add(nested_prefix)
            paths.update(_json_shape(nested_value, nested_prefix))
        return paths
    if isinstance(value, list):
        list_prefix = f'{prefix}[]' if prefix else '[]'
        paths.add(list_prefix)
        for nested_value in value[:10]:
            paths.update(_json_shape(nested_value, list_prefix))
        return paths
    if prefix:
        paths.add(f'{prefix}<{type(value).__name__}>')
    return paths


def _response_shape_signature(response):
    try:
        payload = response.json()
    except json.JSONDecodeError:
        return {'<non-json>', f'<len:{len(response.text)}>'}
    return _json_shape(payload)


def _pretty_response_payload(response):
    try:
        return json.dumps(response.json(), ensure_ascii=False, indent=2, sort_keys=True)
    except json.JSONDecodeError:
        return response.text


def test_excessive_info_endpoints_are_accessible_for_database_without_database_context_now(
    ydb_cluster_with_enforce_user_token,
):
    database_without_db = _collect_statuses(
        ydb_cluster_with_enforce_user_token,
        'database@builtin',
        with_database=False,
    )
    database_with_db = _collect_statuses(
        ydb_cluster_with_enforce_user_token,
        'database@builtin',
        with_database=True,
    )
    root_without_db = _collect_statuses(
        ydb_cluster_with_enforce_user_token,
        'root@builtin',
        with_database=False,
    )

    print('\nStatuses for database@builtin without database:')
    for endpoint, status in database_without_db.items():
        print(f'{endpoint}: {status}')

    print('\nStatuses for database@builtin with database=%2FRoot:')
    for endpoint, status in database_with_db.items():
        print(f'{endpoint}: {status}')

    print('\nStatuses for root@builtin without database:')
    for endpoint, status in root_without_db.items():
        print(f'{endpoint}: {status}')

    leaked_without_database = [endpoint for endpoint, status in database_without_db.items() if status == 200]
    assert leaked_without_database, (
        'Expected to reproduce the issue: database@builtin should currently get 200 '
        'for at least one excessive-info endpoint without database context.'
    )


def test_excessive_info_endpoints_response_shape_is_the_same_for_database_and_root_without_database(
    ydb_cluster_with_enforce_user_token,
):
    database_without_db = _collect_responses(
        ydb_cluster_with_enforce_user_token,
        'database@builtin',
        with_database=False,
    )
    root_without_db = _collect_responses(
        ydb_cluster_with_enforce_user_token,
        'root@builtin',
        with_database=False,
    )

    print('\nResponse shape comparison (database@builtin vs root@builtin, both without database):')
    for endpoint in EXCESSIVE_INFO_ENDPOINTS:
        db_response = database_without_db[endpoint]
        root_response = root_without_db[endpoint]

        assert db_response.status_code == 200, f'{endpoint}: database status={db_response.status_code}'
        assert root_response.status_code == 200, f'{endpoint}: root status={root_response.status_code}'

        db_shape = _response_shape_signature(db_response)
        root_shape = _response_shape_signature(root_response)
        only_in_db = sorted(db_shape - root_shape)
        only_in_root = sorted(root_shape - db_shape)

        print(f'\n{endpoint} database@builtin payload:')
        print(_pretty_response_payload(db_response))
        print(f'\n{endpoint} root@builtin payload:')
        print(_pretty_response_payload(root_response))
        print(
            f'{endpoint}: '
            f'database_shape={len(db_shape)} root_shape={len(root_shape)} '
            f'only_in_db={len(only_in_db)} only_in_root={len(only_in_root)}'
        )

        assert not only_in_db, (
            f'{endpoint}: fields only visible for database token: {only_in_db[:20]}'
        )
        assert not only_in_root, (
            f'{endpoint}: fields only visible for root token: {only_in_root[:20]}'
        )


@pytest.mark.xfail(reason='Desired policy: database token must require database context', strict=False)
def test_excessive_info_endpoints_require_database_context_for_database_token(
    ydb_cluster_with_enforce_user_token,
):
    database_without_db = _collect_statuses(
        ydb_cluster_with_enforce_user_token,
        'database@builtin',
        with_database=False,
    )
    assert all(status != 200 for status in database_without_db.values())


def test_viewer_graph_returns_forbidden_for_database_token(
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
    assert response.status_code == 200, response.text
