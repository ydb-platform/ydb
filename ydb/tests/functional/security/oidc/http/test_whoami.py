import os
import secrets

import pytest
import requests


def whoami(token=None):
    return requests.get(
        f"http://localhost:{os.environ['YDB_MON_PORT']}/viewer/whoami",
        params={'database': os.environ['YDB_DATABASE']},
        headers={'Authorization': 'Bearer ' + token} if token is not None else {},
        allow_redirects=False,
        timeout=10,
    )


def create_keycloak_user(groups, *, request_groups_scope=True):
    username = 'whoami-' + secrets.token_hex(8)
    password = secrets.token_urlsafe(24)
    with requests.Session() as keycloak:
        keycloak.verify = os.environ['OIDC_CA_FILE']
        response = keycloak.post(
            os.environ['OIDC_ADMIN_USERS_URL'],
            headers={'Authorization': 'Bearer ' + os.environ['OIDC_ADMIN_TOKEN']},
            json={
                'username': username,
                'enabled': True,
                'firstName': 'Whoami',
                'lastName': 'Test',
                'email': f'{username}@example.test',
                'emailVerified': True,
                'groups': ['/' + group for group in groups],
                'credentials': [{'type': 'password', 'value': password, 'temporary': False}],
            },
            timeout=10,
        )
        response.raise_for_status()
        response = keycloak.post(
            os.environ['OIDC_ISSUER'] + '/protocol/openid-connect/token',
            data={
                'grant_type': 'password',
                'client_id': 'whoami',
                'username': username,
                'password': password,
                'scope': 'openid ' + os.environ['OIDC_GROUPS_SCOPE'] if request_groups_scope else 'openid',
            },
            timeout=10,
        )
        response.raise_for_status()
        return username, response.json()['access_token']


@pytest.mark.parametrize(('token_env', 'sid_env', 'is_admin'), [
    pytest.param('OIDC_ACCESS_TOKEN', 'OIDC_CLIENT_SID', False, id='service-account'),
    pytest.param('OIDC_CLUSTER_ADMIN_TOKEN', 'YDB_CLUSTER_ADMIN', True, id='cluster-admin'),
])
def test_whoami_accepts_keycloak_token(token_env, sid_env, is_admin):
    response = whoami(os.environ[token_env])
    assert response.status_code == 200, response.text
    identity = response.json()
    assert identity['UserSID'] == f"{os.environ[sid_env]}@{os.environ['OIDC_AUTH_DOMAIN']}"
    assert identity['GroupSIDs'] == ['all-users@well-known']
    assert identity['AuthType'] == 'ExternalIdp'
    assert identity['IsAdministrationAllowed'] is is_admin


@pytest.mark.parametrize(('groups', 'allowed'), [
    pytest.param(('databases',), {'Database'}, id='database'),
    pytest.param(('viewers',), {'Database', 'Viewer'}, id='viewer'),
    pytest.param(('monitorings',), {'Database', 'Viewer', 'Monitoring'}, id='monitoring'),
    pytest.param(('administrations',), {'Database', 'Viewer', 'Monitoring', 'Administration'}, id='administration'),
    pytest.param(('databases', 'unmapped'), {'Database'}, id='database-and-unmapped'),
    pytest.param(('viewers', 'monitorings', 'unmapped'), {'Database', 'Viewer', 'Monitoring'}, id='mixed'),
])
def test_whoami_grants_access_by_keycloak_groups(groups, allowed):
    username, token = create_keycloak_user(groups)
    response = whoami(token)
    assert response.status_code == 200, response.text
    identity = response.json()
    domain = '@' + os.environ['OIDC_AUTH_DOMAIN']
    assert identity['UserSID'] == username + domain
    expected_group_sids = ['all-users@well-known'] + [group + domain for group in groups]
    assert sorted(identity['GroupSIDs']) == sorted(expected_group_sids)
    assert identity['AuthType'] == 'ExternalIdp'
    for role in ('Database', 'Viewer', 'Monitoring', 'Administration'):
        assert identity[f'Is{role}Allowed'] is (role in allowed), role


@pytest.mark.parametrize(('groups', 'request_groups_scope'), [
    pytest.param((), True, id='no-groups'),
    pytest.param(('unmapped',), True, id='unmapped'),
    pytest.param(('administrations',), False, id='administration-without-groups-scope'),
])
def test_whoami_denies_users_without_allowed_group_sids(groups, request_groups_scope):
    _, token = create_keycloak_user(groups, request_groups_scope=request_groups_scope)
    response = whoami(token)
    assert response.status_code == 403, response.text
    assert 'SID is not allowed' in response.text, response.text


def test_whoami_rejects_invalid_signature():
    response = whoami(os.environ['OIDC_BAD_TOKEN'])
    assert response.status_code == 403, response.text


def test_whoami_requires_token():
    response = whoami()
    assert response.status_code == 401, response.text
