# -*- coding: utf-8 -*-
import os
import subprocess

import pytest
import requests
import urllib3

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


TOKENS = [
    None,
    'user@builtin',
    'database@builtin',
    'viewer@builtin',
    'monitoring@builtin',
    'root@builtin',
]


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

    server_key = os.path.join(certs_tmp_dir, 'server.key')
    server_crt = os.path.join(certs_tmp_dir, 'server.crt')
    server_csr = os.path.join(certs_tmp_dir, 'server.csr')
    server_conf = os.path.join(certs_tmp_dir, 'server.conf')

    subprocess.run(['openssl', 'genrsa', '-out', server_key, '2048'], check=True, capture_output=True)

    with open(server_conf, 'w') as config_file:
        config_file.write('[req]\n')
        config_file.write('distinguished_name = req_distinguished_name\n')
        config_file.write('req_extensions = v3_req\n')
        config_file.write('\n')
        config_file.write('[req_distinguished_name]\n')
        config_file.write('\n')
        config_file.write('[v3_req]\n')
        config_file.write('subjectAltName=DNS:localhost,DNS:test-server,IP:127.0.0.1\n')

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


def create_ydb_configurator(certificates):
    cluster_config = {
        'default_clusteradmin': 'root@builtin',
        'enforce_user_token_requirement': True,
    }
    config_generator = KikimrConfigGenerator(**cluster_config)

    if 'grpc_config' not in config_generator.yaml_config:
        config_generator.yaml_config['grpc_config'] = {}
    config_generator.yaml_config['grpc_config']['cert'] = certificates['server_cert']
    config_generator.yaml_config['grpc_config']['key'] = certificates['server_key']

    config_generator.monitoring_tls_cert_path = certificates['server_cert']
    config_generator.monitoring_tls_key_path = certificates['server_key']

    security_config = config_generator.yaml_config['domains_config']['security_config']
    security_config['database_allowed_sids'] = ['database@builtin']
    security_config['viewer_allowed_sids'] = ['viewer@builtin']
    security_config['monitoring_allowed_sids'] = ['monitoring@builtin']

    assert (
        'administration_allowed_sids' in security_config and len(security_config['administration_allowed_sids']) > 0
    ), 'administration_allowed_sids was supposed to be set due to default_clusteradmin'

    return config_generator


@pytest.fixture(scope='module')
def certificates(tmp_path_factory):
    certs_tmp_dir = tmp_path_factory.mktemp('monitoring_certs_')
    return generate_certificates(str(certs_tmp_dir))


@pytest.fixture(scope='module')
def ydb_cluster_with_enforce_user_token(certificates):
    configurator = create_ydb_configurator(certificates)
    cluster = KiKiMR(configurator)
    cluster.start()
    yield cluster
    cluster.stop()


def _test_endpoint(endpoint_url, endpoint_path, token, expected_status):
    headers = {}
    if token is not None:
        headers['Authorization'] = token

    response = requests.get(endpoint_url, headers=headers, verify=False)
    token_desc = token if token is not None else 'null'
    assert (
        response.status_code == expected_status
    ), f'Expected {endpoint_path} with token={token_desc} to return {expected_status}, got {response.status_code}'


def _test_endpoints(cluster, expected_results):
    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    base_url = f'https://{host}:{mon_port}'

    for endpoint_path, expected_statuses in expected_results.items():
        endpoint_url = f'{base_url}{endpoint_path}'
        for token in TOKENS:
            assert token in expected_statuses, f'Missing expected status for {endpoint_path} and token={token}'
            _test_endpoint(endpoint_url, endpoint_path, token, expected_statuses[token])


EXPECTED_RESULTS = {
    # BUG: legacy suite currently treats this endpoint as public in default mode.
    '/actors/tablet_counters_aggregator': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/counters': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/counters/hosts': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/followercounters': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/ping': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/status': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/capabilities': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/healthcheck?format=prometheus': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/login': {
        None: 400,
        'user@builtin': 400,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/monitoring/': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
}


def test_public_access(ydb_cluster_with_enforce_user_token):
    _test_endpoints(ydb_cluster_with_enforce_user_token, EXPECTED_RESULTS)
