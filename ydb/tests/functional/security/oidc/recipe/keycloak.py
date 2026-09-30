"""Scoped Docker lifecycle for the official Keycloak integration fixture."""

import base64
import io
import json
import os
from pathlib import Path
import secrets
import subprocess
import tarfile
import time
import uuid

import requests

IMAGE = 'quay.io/keycloak/keycloak:26.0.7@sha256:4388e2379b7e870a447adbe7b80bd61f5fbf04e925832b19669fda4957f05a81'
CLIENT_ID = 'ydb-service'
DEVICE_CLIENT_ID = 'ydb-cli'
AUDIENCE = 'ydb'
SERVICE_SID = 'service-account-ydb-service@sso'
DEVICE_SID = 'oidc-user@sso'


def _docker(*args, **kwargs):
    # Prefer the local daemon over a stale Docker Desktop context. The Docker
    # daemon and the test process must share the host network namespace.
    env = os.environ.copy()
    if not env.get('DOCKER_HOST') and Path('/var/run/docker.sock').exists():
        env['DOCKER_HOST'] = 'unix:///var/run/docker.sock'
    return subprocess.run(['docker', *map(str, args)], check=True, env=env, timeout=180, **kwargs)


def _realm(secret):
    audience = {
        'name': 'ydb-audience',
        'protocol': 'openid-connect',
        'protocolMapper': 'oidc-audience-mapper',
        'config': {'included.custom.audience': AUDIENCE, 'access.token.claim': 'true', 'id.token.claim': 'false'},
    }
    return {
        'realm': 'production',
        'enabled': True,
        'sslRequired': 'all',
        'accessTokenLifespan': 3600,
        'oauth2DevicePollingInterval': 1,
        'eventsEnabled': True,
        'eventsExpiration': 7200,
        'enabledEventTypes': [
            'CLIENT_LOGIN',
            'OAUTH2_DEVICE_VERIFY_USER_CODE',
            'OAUTH2_DEVICE_CODE_TO_TOKEN',
            'REFRESH_TOKEN',
        ],
        'clients': [
            {
                'clientId': CLIENT_ID,
                'secret': secret,
                'enabled': True,
                'protocol': 'openid-connect',
                'publicClient': False,
                'serviceAccountsEnabled': True,
                'standardFlowEnabled': False,
                'protocolMappers': [audience],
            },
            {
                'clientId': DEVICE_CLIENT_ID,
                'enabled': True,
                'protocol': 'openid-connect',
                'publicClient': True,
                'standardFlowEnabled': False,
                'attributes': {'oauth2.device.authorization.grant.enabled': 'true'},
                'protocolMappers': [audience],
            },
        ],
        'users': [
            {
                'username': 'oidc-user',
                'enabled': True,
                'emailVerified': True,
                'firstName': 'OIDC',
                'lastName': 'User',
                'email': 'oidc-user@example.test',
                'credentials': [{'type': 'password', 'value': 'password', 'temporary': False}],
            },
        ],
    }


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
    realm_path = directory / 'production-realm.json'
    realm_path.write_text(json.dumps(_realm(secret)))
    realm_path.chmod(0o600)
    container = 'ydb-oidc-' + uuid.uuid4().hex
    metadata = directory / 'keycloak-container.json'
    base = f'https://127.0.0.1:{port}'
    issuer = base + '/realms/production'
    try:
        _docker(
            'create',
            '--name',
            container,
            '--label',
            'ydb.test=oidc',
            '--network',
            'host',
            '--memory',
            '1g',
            '--env',
            'JAVA_OPTS_KC_HEAP=-Xms64m -Xmx512m',
            '--env',
            'KC_BOOTSTRAP_ADMIN_USERNAME=admin',
            '--env',
            f'KC_BOOTSTRAP_ADMIN_PASSWORD={admin_password}',
            IMAGE,
            'start-dev',
            '--import-realm',
            '--http-enabled=false',
            '--hostname',
            base,
            '--https-port',
            str(port),
            '--http-host',
            '127.0.0.1',
            '--https-certificate-file=/tmp/server.pem',
            '--https-certificate-key-file=/tmp/server.key',
            stdout=subprocess.DEVNULL,
        )
        metadata.write_text(json.dumps({'container': container}))
        # Copy an archive with explicit ownership before startup, leaving keys
        # readable only by Keycloak (uid 1000), without bind mounts or a
        # chmod/startup race.
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode='w') as files:
            info = tarfile.TarInfo('opt/keycloak/data/import')
            info.type, info.mode, info.uid = tarfile.DIRTYPE, 0o755, 1000
            files.addfile(info)
            for filename, destination in (
                ('server.pem', 'tmp/server.pem'),
                ('server.key', 'tmp/server.key'),
                ('production-realm.json', 'opt/keycloak/data/import/production-realm.json'),
            ):
                data = (directory / filename).read_bytes()
                info = tarfile.TarInfo(destination)
                info.size, info.mode, info.uid = len(data), 0o600, 1000
                files.addfile(info, io.BytesIO(data))
        _docker('cp', '--archive', '-', f'{container}:/', input=archive.getvalue())
        _docker('start', container, stdout=subprocess.DEVNULL)
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
                        raise RuntimeError(f'Keycloak did not start; see {directory / "keycloak.log"}')
                    time.sleep(0.5)
            admin_form = {
                'grant_type': 'password',
                'client_id': 'admin-cli',
                'username': 'admin',
                'password': admin_password,
            }
            token_url = base + '/realms/master/protocol/openid-connect/token'
            response = session.post(token_url, data=admin_form, timeout=15)
            response.raise_for_status()
            # Keep one admin token valid throughout the test suite, including SDK
            # helper subprocesses, without running a separate proxy or issuer.
            response = session.put(
                base + '/admin/realms/master',
                json={'accessTokenLifespan': 7200},
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
                auth=(CLIENT_ID, secret),
                timeout=15,
            )
            response.raise_for_status()
            access_token = response.json()['access_token']
        return {
            'OIDC_ISSUER': issuer,
            'OIDC_CLIENT_ID': CLIENT_ID,
            'OIDC_CLIENT_SECRET': secret,
            'OIDC_DEVICE_CLIENT_ID': DEVICE_CLIENT_ID,
            'OIDC_STATIC_SID': SERVICE_SID,
            'OIDC_CLIENT_SID': SERVICE_SID,
            'OIDC_DEVICE_SID': DEVICE_SID,
            'OIDC_ACCESS_TOKEN': access_token,
            'OIDC_BAD_TOKEN': corrupt_signature(access_token),
            'OIDC_ADMIN_TOKEN': admin_token,
            'OIDC_ADMIN_EVENTS_URL': base + '/admin/realms/production/events',
            'OIDC_CONTAINER_METADATA': str(metadata),
        }
    except BaseException:
        stop(metadata)
        raise


def stop(metadata_path):
    if not metadata_path or not Path(metadata_path).exists():
        return
    metadata = Path(metadata_path)
    container = json.loads(metadata.read_text())['container']
    try:
        with metadata.with_name('keycloak.log').open('wb') as log:
            _docker('logs', container, stdout=log, stderr=subprocess.STDOUT)
    finally:
        _docker('rm', '--force', container, stdout=subprocess.DEVNULL)
        metadata.unlink()
