# -*- coding: utf-8 -*-
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TOKENS = [
    None,
    'user@builtin',
    'database@builtin',
    'viewer@builtin',
    'monitoring@builtin',
    'root@builtin',
]


def _get_base_url(cluster):
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    return f'https://{host}:{mon_port}'


def _check_one_endpoint(base_url, endpoint_path, expected_statuses):
    endpoint_url = f'{base_url}{endpoint_path}'

    for token in TOKENS:
        headers = {}
        if token is not None:
            headers['Authorization'] = token

        response = requests.get(endpoint_url, headers=headers, verify=False)
        token_desc = token if token is not None else 'null'

        assert response.status_code == expected_statuses[token], (
            f'Expected {endpoint_path} with token={token_desc} '
            f'to return {expected_statuses[token]}, got {response.status_code}'
        )


def check_endpoints(cluster, endpoints, expected_statuses):
    base_url = _get_base_url(cluster)

    for endpoint_path in endpoints:
        _check_one_endpoint(base_url, endpoint_path, expected_statuses)


def check_expected_results(cluster, expected_results):
    base_url = _get_base_url(cluster)

    for endpoint_path, expected_statuses in expected_results.items():
        _check_one_endpoint(base_url, endpoint_path, expected_statuses)
