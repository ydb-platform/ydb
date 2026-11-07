from unittest.mock import patch

import pytest
import requests

from ydb.tests.functional.security.oidc.lib.keycloak_client import grant_counts


@pytest.mark.parametrize(
    'change, expected_client, expected_device',
    [('none', 100, 0), ('insert', 100, 1), ('expire', 99, 0)],
)
def test_grant_counts_uses_complete_response(monkeypatch, change, expected_client, expected_device):
    for name, value in {
        'OIDC_CA_FILE': '/unused/ca.pem',
        'OIDC_ADMIN_TOKEN': 'test-admin-token',
        'OIDC_ADMIN_EVENTS_URL': 'https://keycloak.test/events',
        'OIDC_CLIENT_ID': 'sdk-client',
        'OIDC_DEVICE_CLIENT_ID': 'sdk-device',
    }.items():
        monkeypatch.setenv(name, value)

    events = [{'type': 'CLIENT_LOGIN', 'clientId': 'sdk-client'} for _ in range(100)]
    events.append({'type': 'CLIENT_LOGIN', 'clientId': 'cluster-admin'})
    requests_count = 0

    def get(session, url, *, params, timeout):
        nonlocal requests_count
        requests_count += 1
        # Keycloak lists newest events first. Change the list between reads
        # to expose duplicate or missing entries when using offset pagination.
        if requests_count == 2:
            if change == 'insert':
                events.insert(0, {'type': 'OAUTH2_DEVICE_CODE_TO_TOKEN', 'clientId': 'sdk-device'})
            elif change == 'expire':
                del events[0]
        first = params.get('first', 0)
        response = requests.Response()
        response.status_code = 200
        response.json = lambda: events[first:first + params['max']]
        return response

    with patch.object(requests.Session, 'get', get):
        assert grant_counts() == {
            'client_credentials': expected_client,
            'device_verification': 0,
            'device_token': expected_device,
            'refresh_token': 0,
        }
