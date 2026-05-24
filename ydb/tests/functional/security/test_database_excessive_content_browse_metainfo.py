# -*- coding: utf-8 -*-
import pytest
import requests


def _request(base_url, endpoint_path, token):
    return requests.get(
        url=f'{base_url}{endpoint_path}',
        headers={'Authorization': token},
        verify=False,
    )


@pytest.mark.parametrize(
    'endpoint_path',
    [
        '/viewer/browse?path=%2F',
        '/viewer/metainfo?path=%2F',
    ],
)
def test_database_token_can_access_endpoints_without_database_param(
    ydb_cluster_with_enforce_user_token,
    endpoint_path,
):
    host = ydb_cluster_with_enforce_user_token.nodes[1].host
    mon_port = ydb_cluster_with_enforce_user_token.nodes[1].mon_port
    base_url = f'https://{host}:{mon_port}'

    response = _request(
        base_url=base_url,
        endpoint_path=endpoint_path,
        token='database@builtin',
    )
    assert response.status_code == 200, response.text
