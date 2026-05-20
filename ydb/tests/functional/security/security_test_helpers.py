# -*- coding: utf-8 -*-
import requests


def _test_endpoint(endpoint_url, endpoint_path, token, expected_status):
    headers = {}
    if token is not None:
        headers["Authorization"] = token
    response = requests.get(endpoint_url, headers=headers, verify=False)
    token_desc = token if token is not None else "null"
    assert (
        response.status_code == expected_status
    ), f"Expected {endpoint_path} with token={token_desc} to return {expected_status}, got {response.status_code}"


def _test_endpoints(cluster, expected_results):
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    base_url = f"https://{host}:{mon_port}"

    for endpoint_path, expected_statuses in expected_results.items():
        endpoint_url = f"{base_url}{endpoint_path}"
        for token, expected_status in expected_statuses.items():
            _test_endpoint(endpoint_url, endpoint_path, token, expected_status)


def _test_endpoints_via_node_proxy(node, path_suffix, expected_statuses_by_token):
    base_url = f"https://{node.host}:{node.mon_port}"
    node_id = node.node_id
    full_path = f"/node/{node_id}{path_suffix}"
    endpoint_url = f"{base_url}{full_path}"
    for token, expected_status in expected_statuses_by_token.items():
        _test_endpoint(endpoint_url, full_path, token, expected_status)
