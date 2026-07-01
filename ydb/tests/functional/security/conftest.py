# -*- coding: utf-8 -*-
import time

import pytest
import requests

from ydb.tests.library.harness.kikimr_runner import KiKiMR

from cluster_config import create_ydb_configurator, generate_certificates

TENANT_DATABASE = '/Root/Tenant'

pytest_plugins = ['ydb.tests.library.fixtures', 'ydb.tests.library.flavours']


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


def _start_mon_endpoints_auth_cluster(certificates, enforce_user_token_requirement, with_schema_grants):
    configurator = create_ydb_configurator(
        certificates,
        enforce_user_token_requirement=enforce_user_token_requirement,
    )
    cluster = KiKiMR(configurator)
    cluster.start()
    if with_schema_grants:
        _create_tenant_database(cluster, TENANT_DATABASE)
        _grant_describe_schema_on_database(cluster, TENANT_DATABASE)
    return cluster


@pytest.fixture(scope='module')
def ydb_cluster_mon_endpoints_auth_enforce_user_token_enabled_no_schema_grants(certificates):
    cluster = _start_mon_endpoints_auth_cluster(certificates, True, False)
    yield cluster
    cluster.stop()


@pytest.fixture(scope='module')
def ydb_cluster_mon_endpoints_auth_enforce_user_token_disabled_no_schema_grants(certificates):
    cluster = _start_mon_endpoints_auth_cluster(certificates, False, False)
    yield cluster
    cluster.stop()


@pytest.fixture(scope='module')
def ydb_cluster_mon_endpoints_auth_enforce_user_token_enabled_with_schema_grants(certificates):
    cluster = _start_mon_endpoints_auth_cluster(certificates, True, True)
    yield cluster
    cluster.stop()


@pytest.fixture(scope='module')
def ydb_cluster_mon_endpoints_auth_enforce_user_token_disabled_with_schema_grants(certificates):
    cluster = _start_mon_endpoints_auth_cluster(certificates, False, True)
    yield cluster
    cluster.stop()


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


def _start_graph_shard_cluster(configurator):
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
    for _ in range(60):
        described = cluster.client.describe(database, 'root@builtin')
        params = described.PathDescription.DomainDescription.ProcessingParams
        graph_shard_tablet_id = getattr(params, 'GraphShard', None) or getattr(params, 'graph_shard', None)
        if graph_shard_tablet_id:
            break
        time.sleep(1)
    assert graph_shard_tablet_id, 'GraphShard tablet id not available after tenant up'
    cluster.graph_shard_tablet_id = graph_shard_tablet_id
    return cluster


@pytest.fixture(scope='module')
def ydb_cluster_with_enforce_user_token_and_graph_shard(certificates):
    configurator = create_ydb_configurator(
        certificates,
        enforce_user_token_requirement=True,
    )
    cluster = _start_graph_shard_cluster(configurator)
    yield cluster
    cluster.stop()


@pytest.fixture(scope='module')
def ydb_cluster_with_enforce_user_token_secure_devui_flag_and_graph_shard(certificates):
    configurator = create_ydb_configurator(
        certificates,
        enforce_user_token_requirement=True,
        enable_tablet_dev_ui_secure_path=True,
    )
    cluster = _start_graph_shard_cluster(configurator)
    yield cluster
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
