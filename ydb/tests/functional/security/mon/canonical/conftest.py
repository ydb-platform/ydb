# -*- coding: utf-8 -*-
import pytest
import requests

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.functional.security.lib.cluster_config import create_ydb_configurator, generate_certificates

TENANT_DATABASE = '/Root/Tenant'

pytest_plugins = ['ydb.tests.library.fixtures', 'ydb.tests.library.flavours']


@pytest.fixture(scope='module')
def certificates(tmp_path_factory):
    certs_tmp_dir = tmp_path_factory.mktemp('monitoring_certs_')
    return generate_certificates(str(certs_tmp_dir))


def _create_tenant_database(cluster, database_name):
    cluster.create_database(
        database_name,
        storage_pool_units_count={'hdd': 1},
        token='root@builtin',
    )
    cluster.register_and_start_slots(database_name, count=1)
    cluster.wait_tenant_up(database_name, token='root@builtin')


def _grant_describe_schema_on_database(cluster, database_name):
    node = cluster.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    grant_query = (
        f"GRANT 'ydb.granular.describe_schema' ON `{database_name}` "
        f"TO `database@builtin`, `viewer@builtin`, `monitoring@builtin`, `root@builtin`;"
    )
    grant_response = requests.post(
        base_url + '/viewer/query',
        headers={'Authorization': 'root@builtin'},
        params={'database': database_name, 'query': grant_query, 'schema': 'multi'},
        verify=False,
        timeout=30,
    )
    assert grant_response.status_code == 200, grant_response.text


def _start_mon_endpoints_auth_cluster(
    certificates,
    *,
    enforce_user_token_requirement,
    with_schema_grants,
    require_counters_authentication=None,
    require_healthcheck_authentication=None,
):
    configurator = create_ydb_configurator(
        certificates,
        enforce_user_token_requirement=enforce_user_token_requirement,
        require_counters_authentication=require_counters_authentication,
        require_healthcheck_authentication=require_healthcheck_authentication,
        extra_feature_flags=['enable_extra_sids_control_for_http_viewer'],
    )
    cluster = KiKiMR(configurator)
    cluster.start()
    if with_schema_grants:
        _create_tenant_database(cluster, TENANT_DATABASE)
        _grant_describe_schema_on_database(cluster, TENANT_DATABASE)
    return cluster


def _mon_endpoints_auth_cluster_fixture(
    certificates,
    *,
    enforce_user_token_requirement,
    with_schema_grants,
    require_counters_authentication=None,
    require_healthcheck_authentication=None,
):
    cluster = _start_mon_endpoints_auth_cluster(
        certificates,
        enforce_user_token_requirement=enforce_user_token_requirement,
        with_schema_grants=with_schema_grants,
        require_counters_authentication=require_counters_authentication,
        require_healthcheck_authentication=require_healthcheck_authentication,
    )
    yield cluster
    cluster.stop()


@pytest.fixture(scope='module')
def ydb_cluster_mon_endpoints_auth_enforce_user_token_enabled_no_schema_grants(certificates):
    yield from _mon_endpoints_auth_cluster_fixture(
        certificates,
        enforce_user_token_requirement=True,
        with_schema_grants=False,
    )


@pytest.fixture(scope='module')
def ydb_cluster_mon_endpoints_auth_enforce_user_token_enabled_with_schema_grants(certificates):
    yield from _mon_endpoints_auth_cluster_fixture(
        certificates,
        enforce_user_token_requirement=True,
        with_schema_grants=True,
    )


@pytest.fixture(scope='module')
def ydb_cluster_mon_endpoints_auth_enforce_user_token_disabled_no_schema_grants(certificates):
    yield from _mon_endpoints_auth_cluster_fixture(
        certificates,
        enforce_user_token_requirement=False,
        with_schema_grants=False,
    )


@pytest.fixture(scope='module')
def ydb_cluster_mon_endpoints_auth_enforce_user_token_disabled_with_schema_grants(certificates):
    yield from _mon_endpoints_auth_cluster_fixture(
        certificates,
        enforce_user_token_requirement=False,
        with_schema_grants=True,
    )


@pytest.fixture(scope='module')
def ydb_cluster_mon_endpoints_auth_require_counters_authentication_no_schema_grants(certificates):
    yield from _mon_endpoints_auth_cluster_fixture(
        certificates,
        enforce_user_token_requirement=True,
        require_counters_authentication=True,
        with_schema_grants=False,
    )


@pytest.fixture(scope='module')
def ydb_cluster_mon_endpoints_auth_require_counters_authentication_with_schema_grants(certificates):
    yield from _mon_endpoints_auth_cluster_fixture(
        certificates,
        enforce_user_token_requirement=True,
        require_counters_authentication=True,
        with_schema_grants=True,
    )


@pytest.fixture(scope='module')
def ydb_cluster_mon_endpoints_auth_require_healthcheck_authentication_no_schema_grants(certificates):
    yield from _mon_endpoints_auth_cluster_fixture(
        certificates,
        enforce_user_token_requirement=True,
        require_healthcheck_authentication=True,
        with_schema_grants=False,
    )


@pytest.fixture(scope='module')
def ydb_cluster_mon_endpoints_auth_require_healthcheck_authentication_with_schema_grants(certificates):
    yield from _mon_endpoints_auth_cluster_fixture(
        certificates,
        enforce_user_token_requirement=True,
        require_healthcheck_authentication=True,
        with_schema_grants=True,
    )
