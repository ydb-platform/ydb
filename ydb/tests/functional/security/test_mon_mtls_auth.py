# -*- coding: utf-8 -*-
import copy
import os
import subprocess
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context

import pytest

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


class MTLSAdapter(HTTPAdapter):
    """Custom HTTPAdapter that uses client certificates for mTLS"""

    def __init__(self, cert_file, key_file, ca_file, *args, **kwargs):
        self.cert_file = cert_file
        self.key_file = key_file
        self.ca_file = ca_file
        super(MTLSAdapter, self).__init__(*args, **kwargs)

    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        ctx.load_verify_locations(cafile=self.ca_file)
        ctx.load_cert_chain(certfile=self.cert_file, keyfile=self.key_file)
        kwargs['ssl_context'] = ctx
        return super(MTLSAdapter, self).init_poolmanager(*args, **kwargs)


def _generate_client_certificate(certs_tmp_dir, client_name, cn_value, ca_crt, ca_key):
    client_key = os.path.join(certs_tmp_dir, f'{client_name}.key')
    client_crt = os.path.join(certs_tmp_dir, f'{client_name}.crt')
    client_csr = os.path.join(certs_tmp_dir, f'{client_name}.csr')

    subprocess.run(['openssl', 'genrsa', '-out', client_key, '2048'], check=True, capture_output=True)
    subprocess.run(
        [
            'openssl',
            'req',
            '-new',
            '-key',
            client_key,
            '-out',
            client_csr,
            '-subj',
            f'/CN={cn_value}/O=YDB/C=RU',
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
            client_csr,
            '-CA',
            ca_crt,
            '-CAkey',
            ca_key,
            '-CAcreateserial',
            '-out',
            client_crt,
            '-days',
            '3650',
            '-sha256',
        ],
        check=True,
        capture_output=True,
    )
    return client_key, client_crt


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

    admin_client_key, admin_client_crt = _generate_client_certificate(
        certs_tmp_dir, 'admin_client', 'administration_allowed', ca_crt, ca_key
    )

    viewer_client_key, viewer_client_crt = _generate_client_certificate(
        certs_tmp_dir, 'viewer_client', 'viewer_client', ca_crt, ca_key
    )

    return {
        'server_cert': server_crt,
        'server_key': server_key,
        'server_ca': ca_crt,
        'admin_client_crt': admin_client_crt,
        'admin_client_key': admin_client_key,
        'viewer_client_crt': viewer_client_crt,
        'viewer_client_key': viewer_client_key,
    }


CLUSTER_CONFIG = dict(
    enforce_user_token_requirement=True,
    default_clusteradmin='root@builtin',
)


@pytest.fixture(scope='module')
def ydb_cluster_configuration():
    conf = copy.deepcopy(CLUSTER_CONFIG)
    return conf


@pytest.fixture(scope='module')
def certificates(tmp_path_factory):
    certs_tmp_dir = tmp_path_factory.mktemp('monitoring_certs_')
    return generate_certificates(str(certs_tmp_dir))


@pytest.fixture(scope='module')
def client_certificates(certificates):
    """Extract client certificates from certificates"""
    return {
        'admin_cert': certificates['admin_client_crt'],
        'admin_key': certificates['admin_client_key'],
        'viewer_cert': certificates['viewer_client_crt'],
        'viewer_key': certificates['viewer_client_key'],
        'ca': certificates['server_ca'],
    }


@pytest.fixture(scope='module')
def ydb_configurator(ydb_cluster_configuration, certificates):
    config_generator = KikimrConfigGenerator(**ydb_cluster_configuration)

    config_generator.yaml_config['client_certificate_authorization'] = {
        'client_certificate_definitions': [
            {
                'member_groups': ['AdministrationClientAuth@cert'],
                'subject_terms': [{'short_name': 'CN', 'values': ['administration_allowed']}],
            },
            {
                'member_groups': ['ViewerClientAuth@cert'],
                'subject_terms': [{'short_name': 'CN', 'values': ['viewer_client']}],
            },
        ]
    }

    security_config = config_generator.yaml_config['domains_config']['security_config']

    if 'administration_allowed_sids' not in security_config:
        security_config['administration_allowed_sids'] = []
    security_config['administration_allowed_sids'].append('AdministrationClientAuth@cert')
    if 'monitoring_allowed_sids' not in security_config:
        security_config['monitoring_allowed_sids'] = []
    security_config['monitoring_allowed_sids'].append('AdministrationClientAuth@cert')

    if 'grpc_config' not in config_generator.yaml_config:
        config_generator.yaml_config['grpc_config'] = {}
    config_generator.yaml_config['grpc_config']['cert'] = certificates['server_cert']
    config_generator.yaml_config['grpc_config']['key'] = certificates['server_key']
    config_generator.yaml_config['grpc_config']['ca'] = certificates['server_ca']

    config_generator.monitoring_tls_cert_path = certificates['server_cert']
    config_generator.monitoring_tls_key_path = certificates['server_key']
    config_generator.monitoring_tls_ca_path = certificates['server_ca']

    return config_generator


def _test_endpoint_with_certificate(trace_endpoint, client_certificates, cert_type, expected_status):
    session = requests.Session()
    adapter = MTLSAdapter(
        cert_file=client_certificates[f'{cert_type}_cert'],
        key_file=client_certificates[f'{cert_type}_key'],
        ca_file=client_certificates['ca'],
    )
    session.mount('https://', adapter)
    response = session.get(trace_endpoint, verify=client_certificates['ca'], timeout=5)
    assert (
        response.status_code == expected_status
    ), f"Expected /trace with {cert_type} cert to return {expected_status}, got {response.status_code}"


def _test_endpoint_without_any_auth(trace_endpoint):
    response = requests.get(trace_endpoint, timeout=5, verify=False)
    assert (
        response.status_code == 401
    ), f"Expected /trace without auth to return 401, got {response.status_code}"


def _test_endpoint_with_token(trace_endpoint, token, expected_status):
    headers = {'Authorization': token}
    response = requests.get(trace_endpoint, headers=headers, timeout=5, verify=False)
    assert (
        response.status_code == expected_status
    ), f"Expected /trace with token {token} to return {expected_status}, got {response.status_code}"


def test_mtls_auth(ydb_cluster, client_certificates):
    host = ydb_cluster.nodes[1].host
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f'https://{host}:{mon_port}'
    trace_endpoint = f'{base_url}/trace'

    # Without any authentication
    _test_endpoint_without_any_auth(trace_endpoint)

    # With viewer certificate
    _test_endpoint_with_certificate(trace_endpoint, client_certificates, 'viewer', 403)

    # With administration certificate
    _test_endpoint_with_certificate(trace_endpoint, client_certificates, 'admin', 200)


def test_token_auth_when_mtls_is_enabled(ydb_cluster, client_certificates):
    host = ydb_cluster.nodes[1].host
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f'https://{host}:{mon_port}'
    trace_endpoint = f'{base_url}/trace'

    # Checks that mTLS is enabled
    _test_endpoint_with_certificate(trace_endpoint, client_certificates, 'admin', 200)

    # Without any authentication
    _test_endpoint_without_any_auth(trace_endpoint)

    # With NOT an admin token
    _test_endpoint_with_token(trace_endpoint, 'user@builtin', 403)

    # With an admin token
    _test_endpoint_with_token(trace_endpoint, 'root@builtin', 200)
