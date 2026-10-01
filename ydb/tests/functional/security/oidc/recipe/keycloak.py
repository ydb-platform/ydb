"""Keycloak configuration and lifecycle using the shared Docker Compose recipe."""

import base64
import json
import logging
import os
from pathlib import Path
import secrets
import shutil
import time
import uuid

import requests
import yatest.common

from library.python.testing.recipe import set_env
from library.recipes.docker_compose import lib as docker_compose


def corrupt_signature(token):
    parts = token.split('.')
    signature = bytearray(base64.urlsafe_b64decode(parts[2] + '=' * (-len(parts[2]) % 4)))
    signature[0] ^= 1
    parts[2] = base64.urlsafe_b64encode(signature).rstrip(b'=').decode('ascii')
    return '.'.join(parts)


def start(directory, port):
    directory = Path(directory)
    secret = secrets.token_urlsafe(24)
    admin_password = secrets.token_urlsafe(24)
    context = directory / 'keycloak.d'
    source = Path(yatest.common.source_path('ydb/tests/functional/security/oidc/recipe'))
    shutil.copytree(source / 'keycloak.d', context, dirs_exist_ok=True)
    for filename in ('server.pem', 'server.key'):
        shutil.copy2(directory / filename, context / filename)
    marker = directory / 'compose-started'
    for name, value in {
        'COMPOSE_PROJECT_NAME': 'ydb-oidc-' + uuid.uuid4().hex,
        'DOCKER_COMPOSE_FILE': str(source / 'docker-compose.yml'),
        'OIDC_KEYCLOAK_CONTEXT': str(context),
        'OIDC_KEYCLOAK_PORT': str(port),
        'OIDC_KEYCLOAK_ADMIN_PASSWORD': admin_password,
        'OIDC_CLIENT_SECRET': secret,
        'OIDC_COMPOSE_MARKER': str(marker),
    }.items():
        os.environ[name] = value
        set_env(name, value)
    base = f'https://127.0.0.1:{port}'
    realm_url = '/realms/' + os.environ['OIDC_REALM']
    issuer = base + realm_url
    try:
        # Record startup before Compose runs so partially started projects are
        # cleaned up too. The marker makes repeated recipe cleanup harmless.
        marker.touch()
        docker_compose.start([])
        with requests.Session() as session:
            session.verify = str(directory / 'ca.pem')
            deadline = time.monotonic() + 120
            while True:
                try:
                    response = session.get(issuer + '/.well-known/openid-configuration', timeout=2)
                    response.raise_for_status()
                    assert response.json()['issuer'] == issuer
                    break
                except requests.RequestException:
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f'Keycloak did not start; see {yatest.common.output_path("containers")}')
                    time.sleep(0.5)
            admin_form = {
                'grant_type': 'password',
                'client_id': 'admin-cli',
                'username': os.environ['OIDC_ADMIN_USERNAME'],
                'password': admin_password,
            }
            token_url = base + '/realms/master/protocol/openid-connect/token'
            response = session.post(token_url, data=admin_form, timeout=15)
            response.raise_for_status()
            # Keep one admin token valid throughout the test suite, including SDK
            # helper subprocesses, without running a separate proxy or issuer.
            response = session.put(
                base + '/admin/realms/master',
                json=json.loads((source / 'keycloak.d' / 'master-realm-settings.json').read_text()),
                headers={'Authorization': 'Bearer ' + response.json()['access_token']},
                timeout=15,
            )
            response.raise_for_status()
            response = session.post(token_url, data=admin_form, timeout=15)
            response.raise_for_status()
            admin_token = response.json()['access_token']
            response = session.post(
                issuer + '/protocol/openid-connect/token',
                data={'grant_type': 'client_credentials'},
                auth=(os.environ['OIDC_CLIENT_ID'], secret),
                timeout=15,
            )
            response.raise_for_status()
            access_token = response.json()['access_token']
        return {
            'OIDC_ISSUER': issuer,
            'OIDC_ACCESS_TOKEN': access_token,
            'OIDC_BAD_TOKEN': corrupt_signature(access_token),
            'OIDC_ADMIN_TOKEN': admin_token,
            'OIDC_ADMIN_EVENTS_URL': base + '/admin' + realm_url + '/events',
        }
    except BaseException:
        try:
            stop()
        except Exception:
            logging.exception('Failed to stop the Keycloak Compose project after startup failure')
        raise


def stop():
    marker_path = os.environ.get('OIDC_COMPOSE_MARKER')
    if marker_path is None or not Path(marker_path).exists():
        return
    try:
        docker_compose.stop([])
    finally:
        Path(marker_path).unlink(missing_ok=True)
