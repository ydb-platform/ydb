# -*- coding: utf-8 -*-
import os
import subprocess
import tempfile
import urllib3

import pytest
import requests

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def generate_certificates(certs_tmp_dir):
    ca_key = os.path.join(certs_tmp_dir, 'server_ca.key')
    ca_crt = os.path.join(certs_tmp_dir, 'server_ca.crt')

    subprocess.run(['openssl', 'genrsa', '-out', ca_key, '2048'], check=True, capture_output=True)

    subprocess.run(
        [
            'openssl',
            'req',
            '-x509',
            '-new',
            '-nodes',
            '-key',
            ca_key,
            '-sha256',
            '-days',
            '3650',
            '-out',
            ca_crt,
            '-subj',
            '/CN=Monitoring CA/O=YDB/C=RU',
        ],
        check=True,
        capture_output=True,
    )

    # Generate server certificate with localhost in SAN
    server_key = os.path.join(certs_tmp_dir, 'server.key')
    server_crt = os.path.join(certs_tmp_dir, 'server.crt')
    server_csr = os.path.join(certs_tmp_dir, 'server.csr')
    server_conf = os.path.join(certs_tmp_dir, 'server.conf')

    subprocess.run(['openssl', 'genrsa', '-out', server_key, '2048'], check=True, capture_output=True)

    # Create OpenSSL config file with v3_req section and SAN
    with open(server_conf, 'w') as f:
        f.write('[req]\n')
        f.write('distinguished_name = req_distinguished_name\n')
        f.write('req_extensions = v3_req\n')
        f.write('\n')
        f.write('[req_distinguished_name]\n')
        f.write('\n')
        f.write('[v3_req]\n')
        f.write('subjectAltName=DNS:localhost,DNS:test-server,IP:127.0.0.1\n')

    # Generate CSR
    subprocess.run(
        [
            'openssl',
            'req',
            '-new',
            '-key',
            server_key,
            '-out',
            server_csr,
            '-subj',
            '/CN=test-server/O=YDB/C=RU',
            '-config',
            server_conf,
        ],
        check=True,
        capture_output=True,
    )

    # Generate server certificate signed by CA
    subprocess.run(
        [
            'openssl',
            'x509',
            '-req',
            '-in',
            server_csr,
            '-CA',
            ca_crt,
            '-CAkey',
            ca_key,
            '-CAcreateserial',
            '-out',
            server_crt,
            '-days',
            '3650',
            '-sha256',
            '-extensions',
            'v3_req',
            '-extfile',
            server_conf,
        ],
        check=True,
        capture_output=True,
    )

    return {
        'server_cert': server_crt,
        'server_key': server_key,
    }


def create_ydb_configurator(
    certificates,
    enforce_user_token_requirement=True,
):
    cluster_config = {
        'default_clusteradmin': 'root@builtin',
        'enforce_user_token_requirement': enforce_user_token_requirement,
    }
    config_generator = KikimrConfigGenerator(**cluster_config)

    if 'grpc_config' not in config_generator.yaml_config:
        config_generator.yaml_config['grpc_config'] = {}
    config_generator.yaml_config['grpc_config']['cert'] = certificates['server_cert']
    config_generator.yaml_config['grpc_config']['key'] = certificates['server_key']

    config_generator.monitoring_tls_cert_path = certificates['server_cert']
    config_generator.monitoring_tls_key_path = certificates['server_key']

    return config_generator


@pytest.fixture(scope='module')
def certificates():
    certs_tmp_dir = tempfile.mkdtemp(prefix='monitoring_certs_')
    return generate_certificates(certs_tmp_dir)


@pytest.fixture(scope='module')
def ydb_cluster_with_enforce_user_token(certificates):
    configurator = create_ydb_configurator(
        certificates,
        enforce_user_token_requirement=True,
    )
    cluster = KiKiMR(configurator)
    cluster.start()
    yield cluster
    cluster.stop()


@pytest.fixture(scope='module')
def ydb_cluster_without_enforce_user_token(certificates):
    configurator = create_ydb_configurator(
        certificates,
        enforce_user_token_requirement=False,
    )
    cluster = KiKiMR(configurator)
    cluster.start()
    yield cluster
    cluster.stop()


EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN = {
    '/counters': {None: 200, 'user@builtin': 200, 'root@builtin': 200},
    '/counters/hosts': {None: 200, 'user@builtin': 200, 'root@builtin': 200},
    '/healthcheck?&format=prometheus': {None: 200, 'user@builtin': 200, 'root@builtin': 200},
    '/healthcheck': {None: 200, 'user@builtin': 200, 'root@builtin': 200},
    '/ping': {None: 200, 'user@builtin': 200, 'root@builtin': 200},
    '/status': {None: 200, 'user@builtin': 200, 'root@builtin': 200},
    '/ver': {None: 401, 'user@builtin': 403, 'root@builtin': 200},
    '/login': {None: 400, 'user@builtin': 400, 'root@builtin': 400},
    '/viewer/capabilities': {None: 200, 'user@builtin': 200, 'root@builtin': 200},
    '/static/css/bootstrap.min.css': {None: 401, 'user@builtin': 403, 'root@builtin': 200},
    '/monitoring/': {None: 200, 'user@builtin': 200, 'root@builtin': 200},
    '/internal': {None: 401, 'user@builtin': 403, 'root@builtin': 200},
    '/actors/': {None: 401, 'user@builtin': 403, 'root@builtin': 200},
}

EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN = {
    '/counters': {None: 200, 'user@builtin': 200, 'root@builtin': 200},
    '/counters/hosts': {None: 200, 'user@builtin': 200, 'root@builtin': 200},
    '/healthcheck?&format=prometheus': {None: 200, 'user@builtin': 200, 'root@builtin': 200},
    '/healthcheck': {None: 200, 'user@builtin': 200, 'root@builtin': 200},
    '/ping': {None: 200, 'user@builtin': 200, 'root@builtin': 200},
    '/status': {None: 200, 'user@builtin': 200, 'root@builtin': 200},
    '/ver': {None: 200, 'user@builtin': 403, 'root@builtin': 200},
    '/login': {None: 400, 'user@builtin': 400, 'root@builtin': 400},
    '/viewer/capabilities': {None: 200, 'user@builtin': 200, 'root@builtin': 200},
    '/static/css/bootstrap.min.css': {None: 200, 'user@builtin': 403, 'root@builtin': 200},
    '/monitoring/': {None: 200, 'user@builtin': 200, 'root@builtin': 200},
    '/internal': {None: 200, 'user@builtin': 403, 'root@builtin': 200},
    '/actors/': {None: 200, 'user@builtin': 403, 'root@builtin': 200},
}


assert len(EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN) == len(
    EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN
), "Handlers list must be the same"


def _test_endpoint(endpoint_url, endpoint_path, token, expected_status):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    response = requests.get(endpoint_url, headers=headers, verify=False)
    token_desc = token if token is not None else "null"
    assert (
        response.status_code == expected_status
    ), f"Expected {endpoint_path} with token={token_desc} to return {expected_status}, got {response.status_code}"


def _test_endpoints(cluster, expected_results):
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    base_url = f'https://{host}:{mon_port}'

    for endpoint_path, expected_statuses in expected_results.items():
        endpoint_url = f'{base_url}{endpoint_path}'
        for token, expected_status in expected_statuses.items():
            _test_endpoint(endpoint_url, endpoint_path, token, expected_status)


def test_with_enforce_user_token(ydb_cluster_with_enforce_user_token):
    _test_endpoints(ydb_cluster_with_enforce_user_token, EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN)


def test_without_enforce_user_token(ydb_cluster_without_enforce_user_token):
    _test_endpoints(ydb_cluster_without_enforce_user_token, EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN)
