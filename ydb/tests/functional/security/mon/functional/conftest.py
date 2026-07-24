# -*- coding: utf-8 -*-
import time

import pytest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.functional.security.lib.cluster_config import create_ydb_configurator, generate_certificates
from ydb.tests.functional.security.lib.security_test_helpers import mon_base_url as get_mon_base_url
from ydb.tests.functional.security.lib.security_test_helpers import grant_describe_schema_provided

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


@pytest.fixture
def mon_base_url_with_extra_sids_control(ydb_cluster_with_extra_sids_controls):
    return get_mon_base_url(ydb_cluster_with_extra_sids_controls)


@pytest.fixture
def describe_schema_grants(mon_base_url_with_extra_sids_control):
    with grant_describe_schema_provided(mon_base_url_with_extra_sids_control):
        yield


@pytest.fixture(scope='module')
def ydb_cluster_with_extra_sids_controls(certificates):
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
