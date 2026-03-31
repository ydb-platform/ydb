# -*- coding: utf-8 -*-
import os
import subprocess
import urllib3

import pytest
import requests

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

RUNTIME_TOKENS_ORDER = [
    None,
    'random-invalid-token',
    'user@builtin',
    'database@builtin',
    'viewer@builtin',
    'monitoring@builtin',
    'root@builtin',
]

HIERARCHY_TOKENS_ORDER = [
    None,
    'random-invalid-token',
    'database@builtin',
    'viewer@builtin',
    'monitoring@builtin',
    'root@builtin',
]

HIERARCHY_TOKEN_LABELS = {
    None: 'no_token',
    'random-invalid-token': 'random_token',
    'database@builtin': 'database',
    'viewer@builtin': 'viewer',
    'monitoring@builtin': 'monitoring',
    'root@builtin': 'administration',
}

VALID_MONOTONIC_GROUPS_IN_ORDER = [
    (True, True, True, True, True, True),
    (False, True, True, True, True, True),
    (False, False, True, True, True, True),
    (False, False, False, True, True, True),
    (False, False, False, False, True, True),
    (False, False, False, False, False, True),
    (False, False, False, False, False, False),
]

VALID_MONOTONIC_GROUP_RANK = {group: rank for rank, group in enumerate(VALID_MONOTONIC_GROUPS_IN_ORDER)}


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
    require_counters_authentication=None,
    require_healthcheck_authentication=None,
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

    config_generator.yaml_config['domains_config']['security_config']['database_allowed_sids'] = ['database@builtin']
    config_generator.yaml_config['domains_config']['security_config']['viewer_allowed_sids'] = ['viewer@builtin']
    config_generator.yaml_config['domains_config']['security_config']['monitoring_allowed_sids'] = [
        'monitoring@builtin'
    ]
    assert (
        'administration_allowed_sids' in config_generator.yaml_config['domains_config']['security_config']
        and len(config_generator.yaml_config['domains_config']['security_config']['administration_allowed_sids']) > 0
    ), "administration_allowed_sids was supposed to be set due to default_clusteradmin"

    if require_counters_authentication is not None or require_healthcheck_authentication is not None:
        if 'monitoring_config' not in config_generator.yaml_config:
            config_generator.yaml_config['monitoring_config'] = {}
        if require_counters_authentication is not None:
            config_generator.yaml_config['monitoring_config'][
                'require_counters_authentication'
            ] = require_counters_authentication
        if require_healthcheck_authentication is not None:
            config_generator.yaml_config['monitoring_config'][
                'require_healthcheck_authentication'
            ] = require_healthcheck_authentication

    return config_generator


@pytest.fixture(scope='module')
def certificates(tmp_path_factory):
    certs_tmp_dir = tmp_path_factory.mktemp('monitoring_certs_')
    return generate_certificates(str(certs_tmp_dir))


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


@pytest.fixture(scope='module')
def ydb_cluster_with_require_counters_auth(certificates):
    configurator = create_ydb_configurator(
        certificates,
        enforce_user_token_requirement=True,
        require_counters_authentication=True,
    )
    cluster = KiKiMR(configurator)
    cluster.start()
    yield cluster
    cluster.stop()


@pytest.fixture(scope='module')
def ydb_cluster_with_require_healthcheck_auth(certificates):
    configurator = create_ydb_configurator(
        certificates,
        enforce_user_token_requirement=True,
        require_healthcheck_authentication=True,
    )
    cluster = KiKiMR(configurator)
    cluster.start()
    yield cluster
    cluster.stop()


EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN = {
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
    '/labeledcounters': {
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
    '/viewer/bscontrollerinfo': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/cluster': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/compute': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/config': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/counters': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/hiveinfo': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/hivestats': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/labeledcounters': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/netinfo': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/plan2svg': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/pqconsumerinfo': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/storage': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/storage_usage': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/tenants': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/topicinfo': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/storage/groups': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/autocomplete': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/bsgroupinfo': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/feature_flags': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/groups': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/multipart_counter': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/nodeinfo': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/nodelist': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/nodes': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/pdiskinfo': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/peers': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/simple_counter': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/sse_counter': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/sysinfo': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/tabletinfo': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/tenantinfo': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/v2/json/nodeinfo': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/v2/json/pdiskinfo': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/v2/json/sysinfo': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/v2/json/tabletinfo': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/v2/json/vdiskinfo': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/vdiskinfo': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/whoami': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/healthcheck': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/ver': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/static/css/bootstrap.min.css': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/configs_dispatcher': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/console_configs_provider': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/dnameserver': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/dsproxynode': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/feature_flags': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/icb': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/kqp_node': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/kqp_proxy': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/kqp_resource_manager': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/kqp_spilling_file': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/logger': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/memory_tracker': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/netclassifier': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/nodewarden': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/pql2': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/quoter_proxy': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/rb': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/statservice': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/tenant_pool': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/cms': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/grpc': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/internal': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/jquery.tablesorter.css': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/jquery.tablesorter.js': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/memory/fragmentation': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/memory/heap': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/memory/peakheap': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/memory/statistics': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/nodetabmon': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/static/fonts/glyphicons-halflings-regular.eot': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/static/fonts/glyphicons-halflings-regular.svg': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/static/fonts/glyphicons-halflings-regular.ttf': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/static/fonts/glyphicons-halflings-regular.woff': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/static/js/bootstrap.min.js': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/static/js/jquery.min.js': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/tablet': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/tablets': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/trace': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/healthcheck': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/v2/json/config': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/v2/json/nodelist': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/v2/json/storage': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/': {
        None: 401,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    # BUG: database healthcheck access pattern is non-monotonic and breaks access-order assumptions.
    # Runtime currently returns: None=200, random-invalid-token=200, user/database/viewer=403,
    # monitoring/root=200. Keep this case out of the main expected access-order table until fixed.
}

BUGGY_HEALTHCHECK_DATABASE_EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN = {
    '/healthcheck?database=%2FRoot': {
        None: 200,
        'random-invalid-token': 200,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
}

BUGGY_PDISK_EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN = {
    # BUG: `/pdisk` is inconsistent with sibling protected viewer pages in without-enforce mode.
    # Runtime currently redirects anonymous and invalid-token users (302) instead of returning 401/403.
    '/pdisk': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
}

BUGGY_QUERY_EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN = {
    # BUG: `/query` is also inconsistent in without-enforce mode.
    # Runtime currently redirects anonymous and invalid-token users (302) instead of returning 401/403.
    '/query': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
}

BUGGY_SCHEME_EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN = {
    # BUG: `/scheme` is also inconsistent in without-enforce mode.
    # Runtime currently redirects anonymous and invalid-token users (302) instead of returning 401/403.
    '/scheme': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
}

BUGGY_STORAGE_EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN = {
    # BUG: `/storage` is also inconsistent in without-enforce mode.
    # Runtime currently redirects anonymous and invalid-token users (302) instead of returning 401/403.
    '/storage': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
}

BUGGY_VDISK_EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN = {
    # BUG: `/vdisk` is also inconsistent in without-enforce mode.
    # Runtime currently redirects anonymous and invalid-token users (302) instead of returning 401/403.
    '/vdisk': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
}

BUGGY_VIEWER_EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN = {
    # BUG: `/viewer` is also inconsistent in without-enforce mode.
    # Runtime currently redirects anonymous and invalid-token users (302) instead of returning 401/403.
    '/viewer': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
}

BUGGY_VIEWER_V2_EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN = {
    # BUG: `/viewer/v2` is also inconsistent in without-enforce mode.
    # Runtime currently redirects anonymous and invalid-token users (302) instead of returning 401/403.
    '/viewer/v2': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
}

EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN = {
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
    '/labeledcounters': {
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
    '/viewer/bscontrollerinfo': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/cluster': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/compute': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/config': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/counters': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/hiveinfo': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/hivestats': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/labeledcounters': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/netinfo': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/plan2svg': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/pqconsumerinfo': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/storage': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/storage_usage': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/tenants': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/topicinfo': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/storage/groups': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/autocomplete': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/bsgroupinfo': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/feature_flags': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/groups': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/multipart_counter': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/nodeinfo': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/nodelist': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/nodes': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/pdiskinfo': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/peers': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/simple_counter': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/sse_counter': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/sysinfo': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/tabletinfo': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/tenantinfo': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/v2/json/nodeinfo': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/v2/json/pdiskinfo': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/v2/json/sysinfo': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/v2/json/tabletinfo': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/v2/json/vdiskinfo': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/vdiskinfo': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/whoami': {
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
    '/healthcheck': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/ver': {
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
    '/static/css/bootstrap.min.css': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/monitoring/': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/configs_dispatcher': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/console_configs_provider': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/dnameserver': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/dsproxynode': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/feature_flags': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/icb': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/kqp_node': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/kqp_proxy': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/kqp_resource_manager': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/kqp_spilling_file': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/logger': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/memory_tracker': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/netclassifier': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/nodewarden': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/pql2': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/quoter_proxy': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/rb': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/statservice': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/tenant_pool': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/cms': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/grpc': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/internal': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/jquery.tablesorter.css': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/jquery.tablesorter.js': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/memory/fragmentation': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/memory/heap': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/memory/peakheap': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/memory/statistics': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/nodetabmon': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/static/fonts/glyphicons-halflings-regular.eot': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/static/fonts/glyphicons-halflings-regular.svg': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/static/fonts/glyphicons-halflings-regular.ttf': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/static/fonts/glyphicons-halflings-regular.woff': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/static/js/bootstrap.min.js': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/static/js/jquery.min.js': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/tablet': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/tablets': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/trace': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/healthcheck': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/v2/json/config': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/v2/json/nodelist': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/viewer/v2/json/storage': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
    '/actors/': {
        None: 200,
        'user@builtin': 200,
        'database@builtin': 200,
        'viewer@builtin': 200,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
}


REDIRECT_LOCATION_EXPECTATIONS = {
    '/': {
        None: 'monitoring/',
        'random-invalid-token': 'monitoring/',
        'user@builtin': 'monitoring/',
        'database@builtin': 'monitoring/',
        'viewer@builtin': 'monitoring/',
        'monitoring@builtin': 'monitoring/',
        'root@builtin': 'monitoring/',
    },
    '/actors/blobstorageproxies': {
        None: 'blobstorageproxies/',
        'random-invalid-token': 'blobstorageproxies/',
        'user@builtin': 'blobstorageproxies/',
        'database@builtin': 'blobstorageproxies/',
        'viewer@builtin': 'blobstorageproxies/',
        'monitoring@builtin': 'blobstorageproxies/',
        'root@builtin': 'blobstorageproxies/',
    },
    '/actors/interconnect': {
        None: 'interconnect/',
        'random-invalid-token': 'interconnect/',
        'user@builtin': 'interconnect/',
        'database@builtin': 'interconnect/',
        'viewer@builtin': 'interconnect/',
        'monitoring@builtin': 'interconnect/',
        'root@builtin': 'interconnect/',
    },
    '/actors/pdisks': {
        None: 'pdisks/',
        'random-invalid-token': 'pdisks/',
        'user@builtin': 'pdisks/',
        'database@builtin': 'pdisks/',
        'viewer@builtin': 'pdisks/',
        'monitoring@builtin': 'pdisks/',
        'root@builtin': 'pdisks/',
    },
    '/actors/vdisks': {
        None: 'vdisks/',
        'random-invalid-token': 'vdisks/',
        'user@builtin': 'vdisks/',
        'database@builtin': 'vdisks/',
        'viewer@builtin': 'vdisks/',
        'monitoring@builtin': 'vdisks/',
        'root@builtin': 'vdisks/',
    },
    '/monitoring': {
        None: 'monitoring/',
        'random-invalid-token': 'monitoring/',
        'user@builtin': 'monitoring/',
        'database@builtin': 'monitoring/',
        'viewer@builtin': 'monitoring/',
        'monitoring@builtin': 'monitoring/',
        'root@builtin': 'monitoring/',
    },
    '/operation': {
        None: 'operation/',
        'random-invalid-token': 'operation/',
        'user@builtin': 'operation/',
        'database@builtin': 'operation/',
        'viewer@builtin': 'operation/',
        'monitoring@builtin': 'operation/',
        'root@builtin': 'operation/',
    },
    '/pdisk': {
        None: 'pdisk/',
        'random-invalid-token': 'pdisk/',
        'user@builtin': 'pdisk/',
        'database@builtin': 'pdisk/',
        'viewer@builtin': 'pdisk/',
        'monitoring@builtin': 'pdisk/',
        'root@builtin': 'pdisk/',
    },
    '/query': {
        None: 'query/',
        'random-invalid-token': 'query/',
        'user@builtin': 'query/',
        'database@builtin': 'query/',
        'viewer@builtin': 'query/',
        'monitoring@builtin': 'query/',
        'root@builtin': 'query/',
    },
    '/scheme': {
        None: 'scheme/',
        'random-invalid-token': 'scheme/',
        'user@builtin': 'scheme/',
        'database@builtin': 'scheme/',
        'viewer@builtin': 'scheme/',
        'monitoring@builtin': 'scheme/',
        'root@builtin': 'scheme/',
    },
    '/storage': {
        None: 'storage/',
        'random-invalid-token': 'storage/',
        'user@builtin': 'storage/',
        'database@builtin': 'storage/',
        'viewer@builtin': 'storage/',
        'monitoring@builtin': 'storage/',
        'root@builtin': 'storage/',
    },
    '/vdisk': {
        None: 'vdisk/',
        'random-invalid-token': 'vdisk/',
        'user@builtin': 'vdisk/',
        'database@builtin': 'vdisk/',
        'viewer@builtin': 'vdisk/',
        'monitoring@builtin': 'vdisk/',
        'root@builtin': 'vdisk/',
    },
    '/viewer': {
        None: 'viewer/',
        'random-invalid-token': 'viewer/',
        'user@builtin': 'viewer/',
        'database@builtin': 'viewer/',
        'viewer@builtin': 'viewer/',
        'monitoring@builtin': 'viewer/',
        'root@builtin': 'viewer/',
    },
    '/viewer/v2': {
        None: 'v2/',
        'random-invalid-token': 'v2/',
        'user@builtin': 'v2/',
        'database@builtin': 'v2/',
        'viewer@builtin': 'v2/',
        'monitoring@builtin': 'v2/',
        'root@builtin': 'v2/',
    },
}

REDIRECT_OR_NON_API_SURFACE_EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN = {
    '/': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
    '/actors/blobstorageproxies': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
    '/actors/interconnect': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
    '/actors/pdisks': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
    '/actors/vdisks': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
    '/monitoring': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
    '/node': {
        None: 400,
        'random-invalid-token': 400,
        'user@builtin': 400,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/operation': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
    '/pdisk': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
    '/query': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
    '/scheme': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
    '/storage': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
    '/vdisk': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
    '/viewer': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
    '/viewer/v2': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
}

REDIRECT_OR_NON_API_SURFACE_EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN = {
    '/': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
    '/actors/blobstorageproxies': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
    '/actors/interconnect': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
    '/actors/pdisks': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
    '/actors/vdisks': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
    '/monitoring': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
    '/node': {
        None: 400,
        'random-invalid-token': 400,
        'user@builtin': 400,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/operation': {
        None: 302,
        'random-invalid-token': 302,
        'user@builtin': 302,
        'database@builtin': 302,
        'viewer@builtin': 302,
        'monitoring@builtin': 302,
        'root@builtin': 302,
    },
}

UNRESOLVED_OR_UNAVAILABLE_IN_CURRENT_ENVIRONMENT_EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN = {
    '/actors/lease': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 404,
        'root@builtin': 404,
    },
    '/actors/row_dispatcher': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 404,
        'root@builtin': 404,
    },
    '/actors/schemeboard': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 404,
        'root@builtin': 404,
    },
    '/actors/sqsgc': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 404,
        'root@builtin': 404,
    },
    '/actors/yq_control_plane_proxy': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 404,
        'root@builtin': 404,
    },
    '/actors/yq_health': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 404,
        'root@builtin': 404,
    },
    '/fq_diag/fetcher': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 404,
        'root@builtin': 404,
    },
    '/fq_diag/local_worker_manager': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 404,
        'root@builtin': 404,
    },
    '/fq_diag/quotas': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 404,
        'root@builtin': 404,
    },
}

UNRESOLVED_OR_UNAVAILABLE_IN_CURRENT_ENVIRONMENT_EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN = {
    '/actors/lease': {
        None: 404,
        'random-invalid-token': 404,
        'user@builtin': 404,
        'database@builtin': 404,
        'viewer@builtin': 404,
        'monitoring@builtin': 404,
        'root@builtin': 404,
    },
    '/actors/row_dispatcher': {
        None: 404,
        'random-invalid-token': 404,
        'user@builtin': 404,
        'database@builtin': 404,
        'viewer@builtin': 404,
        'monitoring@builtin': 404,
        'root@builtin': 404,
    },
    '/actors/schemeboard': {
        None: 404,
        'random-invalid-token': 404,
        'user@builtin': 404,
        'database@builtin': 404,
        'viewer@builtin': 404,
        'monitoring@builtin': 404,
        'root@builtin': 404,
    },
    '/actors/sqsgc': {
        None: 404,
        'random-invalid-token': 404,
        'user@builtin': 404,
        'database@builtin': 404,
        'viewer@builtin': 404,
        'monitoring@builtin': 404,
        'root@builtin': 404,
    },
    '/actors/yq_control_plane_proxy': {
        None: 404,
        'random-invalid-token': 404,
        'user@builtin': 404,
        'database@builtin': 404,
        'viewer@builtin': 404,
        'monitoring@builtin': 404,
        'root@builtin': 404,
    },
    '/actors/yq_health': {
        None: 404,
        'random-invalid-token': 404,
        'user@builtin': 404,
        'database@builtin': 404,
        'viewer@builtin': 404,
        'monitoring@builtin': 404,
        'root@builtin': 404,
    },
    '/fq_diag/fetcher': {
        None: 404,
        'random-invalid-token': 404,
        'user@builtin': 404,
        'database@builtin': 404,
        'viewer@builtin': 404,
        'monitoring@builtin': 404,
        'root@builtin': 404,
    },
    '/fq_diag/local_worker_manager': {
        None: 404,
        'random-invalid-token': 404,
        'user@builtin': 404,
        'database@builtin': 404,
        'viewer@builtin': 404,
        'monitoring@builtin': 404,
        'root@builtin': 404,
    },
    '/fq_diag/quotas': {
        None: 404,
        'random-invalid-token': 404,
        'user@builtin': 404,
        'database@builtin': 404,
        'viewer@builtin': 404,
        'monitoring@builtin': 404,
        'root@builtin': 404,
    },
}


def _derive_random_invalid_token_status(no_token_status, endpoint_path=None):
    if no_token_status == 401 and endpoint_path and endpoint_path.startswith('/healthcheck'):
        return 401
    return 403 if no_token_status == 401 else no_token_status


REQUIRES_PARAMETERS_OR_REQUEST_CONTEXT_EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN = {
    '/operation/cancel': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/operation/forget': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/operation/get': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/operation/list': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/pdisk/info': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/pdisk/restart': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/pdisk/status': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/query/script/execute': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/query/script/fetch': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/scheme/directory': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/vdisk/blobindexstat': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/vdisk/evict': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/vdisk/getblob': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/vdisk/vdiskstat': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/acl': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/browse': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/check_access': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/commit_offset': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/database_stats': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/describe': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/describe_consumer': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/describe_replication': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/describe_topic': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/describe_transfer': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/graph': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/hotkeys': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/metainfo': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/put_record': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/query': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/render': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/storage_stats': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
    '/viewer/topic_data': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
}


BUGGY_REQUIRES_PARAMETERS_OR_REQUEST_CONTEXT_EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN = {
    # BUG: `/viewer/content` may time out on empty requests instead of returning a stable HTTP status.
    '/viewer/content': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 504,
        'viewer@builtin': 504,
        'monitoring@builtin': 504,
        'root@builtin': 504,
    },
    # BUG: `/viewer/tabletcounters` may time out on empty requests before the handler returns a normal HTTP status.
    '/viewer/tabletcounters': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': None,
        'viewer@builtin': None,
        'monitoring@builtin': None,
        'root@builtin': None,
    },
}


OPERATION_LIST_WITH_PARAMETERS_EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN = {
    '/operation/list?database=%2FRoot': {
        None: 401,
        'random-invalid-token': 403,
        'user@builtin': 403,
        'database@builtin': 400,
        'viewer@builtin': 400,
        'monitoring@builtin': 400,
        'root@builtin': 400,
    },
}


def _normalize_expected_statuses(endpoint_path, expected_statuses, derive_random_invalid_token):
    normalized = dict(expected_statuses)

    assert None in normalized, f"Endpoint {endpoint_path} is missing None status"

    if derive_random_invalid_token:
        derived_random_invalid_status = _derive_random_invalid_token_status(normalized[None], endpoint_path)
        if 'random-invalid-token' in normalized:
            assert normalized['random-invalid-token'] == derived_random_invalid_status, (
                f"Endpoint {endpoint_path} has inconsistent random-invalid-token status: "
                f"expected {derived_random_invalid_status}, got {normalized['random-invalid-token']}"
            )
        else:
            normalized['random-invalid-token'] = derived_random_invalid_status
    else:
        assert (
            'random-invalid-token' in normalized
        ), f"Endpoint {endpoint_path} must define explicit random-invalid-token status"

    missing_runtime_tokens = [token for token in RUNTIME_TOKENS_ORDER if token not in normalized]
    assert (
        not missing_runtime_tokens
    ), f"Endpoint {endpoint_path} is missing runtime token statuses for: {missing_runtime_tokens}"

    return normalized


def _normalize_expected_results(expected_results, derive_random_invalid_token):
    return {
        endpoint_path: _normalize_expected_statuses(
            endpoint_path,
            expected_statuses,
            derive_random_invalid_token=derive_random_invalid_token,
        )
        for endpoint_path, expected_statuses in expected_results.items()
    }


def _assert_same_endpoint_keys(left_name, left_results, right_name, right_results):
    left_keys = set(left_results.keys())
    right_keys = set(right_results.keys())

    missing_in_right = sorted(left_keys - right_keys)
    missing_in_left = sorted(right_keys - left_keys)

    assert left_keys == right_keys, (
        f"Endpoint key sets differ:\n"
        f"missing in {right_name}: {missing_in_right}\n"
        f"missing in {left_name}: {missing_in_left}"
    )


def _is_accessible(status_code):
    return status_code not in (401, 403)


def _access_pattern_from_statuses(expected_statuses):
    return tuple(_is_accessible(expected_statuses[token]) for token in HIERARCHY_TOKENS_ORDER)


def _assert_monotonic_access_pattern(endpoint_path, expected_statuses):
    access_pattern = _access_pattern_from_statuses(expected_statuses)

    for left_token, right_token, left_access, right_access in zip(
        HIERARCHY_TOKENS_ORDER,
        HIERARCHY_TOKENS_ORDER[1:],
        access_pattern,
        access_pattern[1:],
    ):
        left_name = HIERARCHY_TOKEN_LABELS[left_token]
        right_name = HIERARCHY_TOKEN_LABELS[right_token]
        assert not left_access or right_access, (
            f"Access hierarchy violation for {endpoint_path}: "
            f"{left_name} is allowed while {right_name} is denied; "
            f"statuses={expected_statuses}"
        )

    return access_pattern


def _assert_access_order(expected_results):
    normalized_results = _normalize_expected_results(
        expected_results,
        derive_random_invalid_token=True,
    )

    encountered_groups = []
    previous_group = None
    previous_rank = -1

    for endpoint_path, expected_statuses in normalized_results.items():
        access_pattern = _assert_monotonic_access_pattern(endpoint_path, expected_statuses)

        assert access_pattern in VALID_MONOTONIC_GROUP_RANK, (
            f"Endpoint {endpoint_path} produced unexpected non-monotonic group {access_pattern}; "
            f"statuses={expected_statuses}"
        )

        current_rank = VALID_MONOTONIC_GROUP_RANK[access_pattern]

        assert current_rank >= previous_rank, (
            f"Unexpected access groups order for endpoint {endpoint_path}: "
            f"group {access_pattern} appears after a stricter group. "
            f"Encountered groups so far: {encountered_groups}"
        )

        if access_pattern != previous_group:
            encountered_groups.append(access_pattern)
            previous_group = access_pattern
            previous_rank = current_rank

    assert encountered_groups, "No access groups were detected"


def _assert_redirect_headers(endpoint_path, token, response):
    expected_location = REDIRECT_LOCATION_EXPECTATIONS[endpoint_path][token]
    actual_location = response.headers.get('Location')
    token_desc = token if token is not None else 'null'
    assert (
        actual_location == expected_location
    ), f"Expected {endpoint_path} with token={token_desc} to redirect to {expected_location}, got {actual_location}"


def _assert_non_redirect_headers(endpoint_path, token, response):
    token_desc = token if token is not None else 'null'
    assert (
        response.headers.get('Location') is None
    ), f"Expected {endpoint_path} with token={token_desc} to have no Location header, got {response.headers.get('Location')}"


def _test_endpoint(endpoint_url, endpoint_path, token, expected_status):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    response = requests.get(endpoint_url, headers=headers, verify=False, allow_redirects=False)
    token_desc = token if token is not None else "null"
    assert (
        response.status_code == expected_status
    ), f"Expected {endpoint_path} with token={token_desc} to return {expected_status}, got {response.status_code}"

    if expected_status in (301, 302, 303, 307, 308):
        assert (
            endpoint_path in REDIRECT_LOCATION_EXPECTATIONS
        ), f"Missing redirect location expectations for {endpoint_path}"
        assert (
            token in REDIRECT_LOCATION_EXPECTATIONS[endpoint_path]
        ), f"Missing redirect location expectation for {endpoint_path} with token={token_desc}"
        _assert_redirect_headers(endpoint_path, token, response)
    elif expected_status in (401, 403):
        _assert_non_redirect_headers(endpoint_path, token, response)


def _test_endpoints(cluster, expected_results, derive_random_invalid_token):
    normalized_results = _normalize_expected_results(
        expected_results,
        derive_random_invalid_token=derive_random_invalid_token,
    )

    host = cluster.nodes[1].host
    mon_port = cluster.nodes[1].mon_port
    base_url = f'https://{host}:{mon_port}'

    for endpoint_path, expected_statuses in normalized_results.items():
        endpoint_url = f'{base_url}{endpoint_path}'
        for token in RUNTIME_TOKENS_ORDER:
            expected_status = expected_statuses[token]
            _test_endpoint(endpoint_url, endpoint_path, token, expected_status)


def test_expected_results_sets_are_equal():
    _assert_same_endpoint_keys(
        'EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN',
        EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN,
        'EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN',
        EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN,
    )


def test_access_order_with_enforce_user_token():
    _assert_access_order(EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN)


def test_access_order_without_enforce_user_token():
    _assert_access_order(EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN)


def test_redirect_or_non_api_surface_with_enforce_user_token(ydb_cluster_with_enforce_user_token):
    _test_endpoints(
        ydb_cluster_with_enforce_user_token,
        REDIRECT_OR_NON_API_SURFACE_EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN,
        derive_random_invalid_token=False,
    )


def test_with_enforce_user_token(ydb_cluster_with_enforce_user_token):
    _test_endpoints(
        ydb_cluster_with_enforce_user_token,
        EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN,
        derive_random_invalid_token=True,
    )
    _test_endpoints(
        ydb_cluster_with_enforce_user_token,
        BUGGY_HEALTHCHECK_DATABASE_EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN,
        derive_random_invalid_token=False,
    )


def test_redirect_or_non_api_surface_without_enforce_user_token(ydb_cluster_without_enforce_user_token):
    _test_endpoints(
        ydb_cluster_without_enforce_user_token,
        REDIRECT_OR_NON_API_SURFACE_EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN,
        derive_random_invalid_token=False,
    )
    _test_endpoints(
        ydb_cluster_without_enforce_user_token,
        BUGGY_PDISK_EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN,
        derive_random_invalid_token=False,
    )
    _test_endpoints(
        ydb_cluster_without_enforce_user_token,
        BUGGY_QUERY_EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN,
        derive_random_invalid_token=False,
    )
    _test_endpoints(
        ydb_cluster_without_enforce_user_token,
        BUGGY_SCHEME_EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN,
        derive_random_invalid_token=False,
    )
    _test_endpoints(
        ydb_cluster_without_enforce_user_token,
        BUGGY_STORAGE_EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN,
        derive_random_invalid_token=False,
    )
    _test_endpoints(
        ydb_cluster_without_enforce_user_token,
        BUGGY_VDISK_EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN,
        derive_random_invalid_token=False,
    )
    _test_endpoints(
        ydb_cluster_without_enforce_user_token,
        BUGGY_VIEWER_EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN,
        derive_random_invalid_token=False,
    )
    _test_endpoints(
        ydb_cluster_without_enforce_user_token,
        BUGGY_VIEWER_V2_EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN,
        derive_random_invalid_token=False,
    )


def test_unresolved_or_unavailable_in_current_environment_with_enforce_user_token(ydb_cluster_with_enforce_user_token):
    _test_endpoints(
        ydb_cluster_with_enforce_user_token,
        UNRESOLVED_OR_UNAVAILABLE_IN_CURRENT_ENVIRONMENT_EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN,
        derive_random_invalid_token=False,
    )


def test_unresolved_or_unavailable_in_current_environment_without_enforce_user_token(
    ydb_cluster_without_enforce_user_token,
):
    _test_endpoints(
        ydb_cluster_without_enforce_user_token,
        UNRESOLVED_OR_UNAVAILABLE_IN_CURRENT_ENVIRONMENT_EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN,
        derive_random_invalid_token=False,
    )


def test_without_enforce_user_token(ydb_cluster_without_enforce_user_token):
    _test_endpoints(
        ydb_cluster_without_enforce_user_token,
        EXPECTED_RESULTS_WITHOUT_ENFORCE_USER_TOKEN,
        derive_random_invalid_token=True,
    )


def test_requires_parameters_or_request_context_with_enforce_user_token(ydb_cluster_with_enforce_user_token):
    _test_endpoints(
        ydb_cluster_with_enforce_user_token,
        REQUIRES_PARAMETERS_OR_REQUEST_CONTEXT_EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN,
        derive_random_invalid_token=True,
    )


@pytest.mark.xfail(reason='Known bug: empty request handling is unstable for these parameter-dependent endpoints')
def test_buggy_requires_parameters_or_request_context_with_enforce_user_token(ydb_cluster_with_enforce_user_token):
    _test_endpoints(
        ydb_cluster_with_enforce_user_token,
        BUGGY_REQUIRES_PARAMETERS_OR_REQUEST_CONTEXT_EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN,
        derive_random_invalid_token=False,
    )


def test_operation_list_with_parameters_with_enforce_user_token(ydb_cluster_with_enforce_user_token):
    _test_endpoints(
        ydb_cluster_with_enforce_user_token,
        OPERATION_LIST_WITH_PARAMETERS_EXPECTED_RESULTS_WITH_ENFORCE_USER_TOKEN,
        derive_random_invalid_token=True,
    )


def test_with_require_counters_authentication(ydb_cluster_with_require_counters_auth):
    EXPECTED_RESULTS_WITH_REQUIRE_COUNTERS_AUTH = {
        '/counters': {
            None: 401,
            'random-invalid-token': 403,
            'user@builtin': 403,
            'database@builtin': 403,
            'viewer@builtin': 200,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        },
        '/counters/hosts': {
            None: 401,
            'random-invalid-token': 403,
            'user@builtin': 403,
            'database@builtin': 403,
            'viewer@builtin': 200,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        },
        '/ping': {
            None: 200,
            'random-invalid-token': 200,
            'user@builtin': 200,
            'database@builtin': 200,
            'viewer@builtin': 200,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        },  # checks this just in case
    }
    _test_endpoints(
        ydb_cluster_with_require_counters_auth,
        EXPECTED_RESULTS_WITH_REQUIRE_COUNTERS_AUTH,
        derive_random_invalid_token=False,
    )


def test_with_require_healthcheck_authentication(ydb_cluster_with_require_healthcheck_auth):
    EXPECTED_RESULTS_WITH_REQUIRE_HEALTHCHECK_AUTH = {
        '/healthcheck?format=prometheus': {
            None: 401,
            'random-invalid-token': 401,
            'user@builtin': 403,
            'database@builtin': 403,
            'viewer@builtin': 200,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        },
        '/healthcheck': {
            None: 401,
            'random-invalid-token': 401,
            'user@builtin': 403,
            'database@builtin': 403,
            'viewer@builtin': 403,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        },
        '/healthcheck?database=%2FRoot': {
            None: 401,
            'random-invalid-token': 401,
            'user@builtin': 403,
            'database@builtin': 403,
            'viewer@builtin': 403,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        },
        '/healthcheck?database=%2FRoot&format=prometheus': {
            None: 401,
            'random-invalid-token': 401,
            'user@builtin': 403,
            'database@builtin': 403,
            'viewer@builtin': 200,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        },
        '/ping': {
            None: 200,
            'random-invalid-token': 200,
            'user@builtin': 200,
            'database@builtin': 200,
            'viewer@builtin': 200,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        },  # checks this just in case
    }
    _test_endpoints(
        ydb_cluster_with_require_healthcheck_auth,
        EXPECTED_RESULTS_WITH_REQUIRE_HEALTHCHECK_AUTH,
        derive_random_invalid_token=False,
    )
