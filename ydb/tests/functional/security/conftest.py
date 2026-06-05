# -*- coding: utf-8 -*-
import time

import pytest

from ydb.tests.library.harness.kikimr_runner import KiKiMR

from cluster_config import create_ydb_configurator, generate_certificates

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
