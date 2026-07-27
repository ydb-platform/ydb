# -*- coding: utf-8 -*-
import pytest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.functional.security.lib.cluster_config import create_ydb_configurator, generate_certificates
from ydb.tests.functional.security.lib.security_test_helpers import run_viewer_query

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
    slots = cluster.register_and_start_slots(database_name, count=1)
    cluster.wait_tenant_up(database_name, token='root@builtin')
    return slots[0]


def _grant_describe_schema_on_database(tenant_node, database_name):
    # Query tenant DB on the tenant slot directly to avoid root-node 307 redirect
    # through /node/<id>/viewer/query, which can hit the legacy EvHttpInfo path.
    base_url = f'https://{tenant_node.host}:{tenant_node.mon_port}'
    run_viewer_query(
        base_url,
        f"GRANT 'ydb.granular.describe_schema' ON `{database_name}` "
        f"TO `database@builtin`, `viewer@builtin`, `monitoring@builtin`, `root@builtin`;",
        database=database_name,
    )


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
        tenant_node = _create_tenant_database(cluster, TENANT_DATABASE)
        _grant_describe_schema_on_database(tenant_node, TENANT_DATABASE)
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
