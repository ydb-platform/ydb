# -*- coding: utf-8 -*-
import os
import subprocess

import pytest

from ydb.tests.library.harness.kikimr_runner import KiKiMR

from cluster_config import create_ydb_configurator

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


_MON_ENDPOINTS_AUTH_CLUSTER_PARAMS = (
    pytest.param(
        {
            'case_name': 'enforce_user_token_enabled',
            'enforce_user_token_requirement': True,
        },
        id='enforce_user_token_enabled',
    ),
    pytest.param(
        {
            'case_name': 'enforce_user_token_disabled',
            'enforce_user_token_requirement': False,
        },
        id='enforce_user_token_disabled',
    ),
    pytest.param(
        {
            'case_name': 'require_counters_authentication',
            'enforce_user_token_requirement': True,
            'require_counters_authentication': True,
        },
        id='require_counters_authentication',
    ),
    pytest.param(
        {
            'case_name': 'require_healthcheck_authentication',
            'enforce_user_token_requirement': True,
            'require_healthcheck_authentication': True,
        },
        id='require_healthcheck_authentication',
    ),
)


@pytest.fixture(scope='module', params=_MON_ENDPOINTS_AUTH_CLUSTER_PARAMS)
def ydb_cluster_for_mon_endpoints_auth(request, certificates):
    params = request.param.copy()
    case_name = params.pop('case_name')
    configurator = create_ydb_configurator(
        certificates,
        **params,
    )
    cluster = KiKiMR(configurator)
    cluster.start()
    yield case_name, cluster
    cluster.stop()


@pytest.fixture(scope='module')
def ydb_cluster_with_external_access_controls(certificates):
    configurator = create_ydb_configurator(
        certificates,
        enforce_user_token_requirement=True,
        extra_feature_flags=['enable_extra_sids_control_for_http_viewer'],
    )
    cluster = KiKiMR(configurator)
    cluster.start()
    yield cluster
    cluster.stop()


@pytest.fixture(scope='module')
def ydb_cluster_with_enforce_user_token_and_tablet_devui_secure_path_flag(certificates):
    configurator = create_ydb_configurator(
        certificates,
        enforce_user_token_requirement=True,
        enable_tablet_dev_ui_secure_path=True,
    )
    cluster = KiKiMR(configurator)
    cluster.start()
    yield cluster
    cluster.stop()
