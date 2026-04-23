# -*- coding: utf-8 -*-
import os
import subprocess
import time

import pytest

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR

pytest_plugins = ['ydb.tests.library.fixtures', 'ydb.tests.library.flavours']


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

    security_config = config_generator.yaml_config['domains_config']['security_config']
    security_config['database_allowed_sids'] = ['database@builtin']
    security_config['viewer_allowed_sids'] = ['viewer@builtin']
    security_config['monitoring_allowed_sids'] = ['monitoring@builtin']

    assert (
        'administration_allowed_sids' in security_config and len(security_config['administration_allowed_sids']) > 0
    ), 'administration_allowed_sids was supposed to be set due to default_clusteradmin'

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
def ydb_cluster_with_enforce_user_token_and_graph_shard(certificates):
    configurator = create_ydb_configurator(
        certificates,
        enforce_user_token_requirement=True,
    )
    configurator.yaml_config.setdefault('feature_flags', {})['enable_graph_shard'] = True
    cluster = KiKiMR(configurator)
    cluster.start()
    database = '/Root/graph_mon_security'
    cluster.create_database(
        database,
        storage_pool_units_count={'hdd': 1},
        token='root@builtin',
    )
    cluster.register_and_start_slots(database, count=1)
    cluster.wait_tenant_up(database, token='root@builtin')

    graph_shard_tablet_id = None
    for _ in range(120):
        described = cluster.client.describe(database, 'root@builtin')
        params = described.PathDescription.DomainDescription.ProcessingParams
        graph_shard_tablet_id = getattr(params, 'GraphShard', None) or getattr(params, 'graph_shard', None)
        if graph_shard_tablet_id:
            break
        time.sleep(1)
    assert graph_shard_tablet_id, 'GraphShard tablet id not available after tenant up'
    cluster.graph_shard_tablet_id = graph_shard_tablet_id

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
