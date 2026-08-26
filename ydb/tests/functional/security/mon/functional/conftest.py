# -*- coding: utf-8 -*-
import time

import pytest
import requests

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.functional.security.lib.cluster_config import create_ydb_configurator, generate_certificates
from ydb.tests.functional.security.lib.security_test_helpers import mon_base_url as get_mon_base_url
from ydb.tests.functional.security.lib.security_test_helpers import grant_describe_schema_provided
from ydb.tests.functional.security.lib.security_test_helpers import get_foreign_node_id_for_database
from ydb.tests.functional.security.lib.security_test_helpers import get_nodelist_ids
from ydb.tests.functional.security.lib.security_test_helpers import get_tenant_path_id
from ydb.tests.functional.security.lib.security_test_helpers import get_tenant_schemeshard_id
from ydb.tests.functional.security.lib.security_test_helpers import get_storage_ids
from ydb.tests.functional.security.lib.security_test_helpers import get_unknown_node_id
from ydb.tests.functional.security.lib.security_test_helpers import run_viewer_query
from ydb.tests.functional.security.lib.security_test_helpers import wait_for_storage_ids
from ydb.tests.functional.security.lib.security_test_helpers import wait_for_viewer_ready
from ydb.tests.oss.ydb_sdk_import import ydb

pytest_plugins = ['ydb.tests.library.fixtures', 'ydb.tests.library.flavours']


@pytest.fixture(scope='module')
def certificates(tmp_path_factory):
    certs_tmp_dir = tmp_path_factory.mktemp('monitoring_certs_')
    return generate_certificates(str(certs_tmp_dir))


def _pers_queue_tablet_id_from_viewer(cluster, database, topic_path):
    node = cluster.nodes[1]
    response = requests.get(
        f'https://{node.host}:{node.mon_port}/viewer/json/describe',
        params={
            'database': database,
            'path': topic_path,
            'partition_stats': 'true',
            'subs': '0',
            'enums': 'true',
        },
        headers={'Authorization': 'root@builtin'},
        verify=False,
        timeout=10,
    )
    response.raise_for_status()
    partitions = response.json()['PathDescription']['PersQueueGroup']['Partitions']
    for partition in partitions:
        tablet_id = partition.get('TabletId')
        if tablet_id:
            return tablet_id
    raise RuntimeError('PersQueue tablet id not found in topic description')


def _start_pers_queue_cluster(configurator):
    cluster = KiKiMR(configurator)
    cluster.start()
    database = '/Root/pq_mon_security'
    topic_path = f'{database}/topic'
    cluster.create_database(database, storage_pool_units_count={'hdd': 1}, token='root@builtin')
    cluster.register_and_start_slots(database, count=1)
    cluster.wait_tenant_up(database, token='root@builtin')

    node = cluster.nodes[1]
    driver_config = ydb.DriverConfig(
        endpoint=f'{node.host}:{node.port}',
        database=database,
        credentials=ydb.AuthTokenCredentials('root@builtin'),
    )
    with ydb.Driver(driver_config) as driver:
        driver.wait(timeout=30)
        session = driver.table_client.session().create()
        session.execute_scheme(f'CREATE TOPIC `{topic_path}`;')

    pers_queue_tablet_id = None
    for _ in range(60):
        try:
            pers_queue_tablet_id = _pers_queue_tablet_id_from_viewer(cluster, database, topic_path)
            break
        except (KeyError, RuntimeError, requests.RequestException):
            time.sleep(1)
    assert pers_queue_tablet_id, 'PersQueue tablet id not available after topic creation'
    cluster.pers_queue_database = database
    cluster.pers_queue_topic_path = topic_path
    cluster.pers_queue_tablet_id = pers_queue_tablet_id
    return cluster


@pytest.fixture(scope='module')
def ydb_cluster_with_enforce_user_token_and_pers_queue_topic(certificates):
    configurator = create_ydb_configurator(
        certificates,
        enforce_user_token_requirement=True,
    )
    cluster = _start_pers_queue_cluster(configurator)
    yield cluster
    cluster.stop()


@pytest.fixture(scope='module')
def ydb_cluster_with_enforce_user_token_secure_devui_flag_and_pers_queue_topic(certificates):
    configurator = create_ydb_configurator(
        certificates,
        enforce_user_token_requirement=True,
        enable_tablet_dev_ui_secure_path=True,
    )
    cluster = _start_pers_queue_cluster(configurator)
    yield cluster
    cluster.stop()


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


TENANT_DATABASE = '/Root/Tenant'


@pytest.fixture(scope='module')
def tenant_database(ydb_cluster_with_extra_sids_controls):
    cluster = ydb_cluster_with_extra_sids_controls
    cluster.create_database(
        TENANT_DATABASE,
        storage_pool_units_count={'hdd': 1},
        token='root@builtin',
    )
    slots = cluster.register_and_start_slots(TENANT_DATABASE, count=1)
    cluster.wait_tenant_up(TENANT_DATABASE, token='root@builtin')
    tenant_node = slots[0]
    wait_for_viewer_ready(
        f'https://{tenant_node.host}:{tenant_node.mon_port}',
        database=TENANT_DATABASE,
    )
    run_viewer_query(
        f'https://{tenant_node.host}:{tenant_node.mon_port}',
        f"GRANT 'ydb.granular.describe_schema' ON `{TENANT_DATABASE}` "
        f"TO `database@builtin`, `viewer@builtin`, `monitoring@builtin`, `root@builtin`;",
        database=TENANT_DATABASE,
    )
    return TENANT_DATABASE


@pytest.fixture(scope='module')
def tenant_describe_ids(ydb_cluster_with_extra_sids_controls, tenant_database):
    cluster = ydb_cluster_with_extra_sids_controls
    return {
        'path_id': get_tenant_path_id(cluster, tenant_database, tenant_database, use_tls=True, token='root@builtin'),
        'schemeshard_id': get_tenant_schemeshard_id(cluster, tenant_database, tenant_database, use_tls=True, token='root@builtin'),
    }


@pytest.fixture(scope='module')
def tenant_nodelist_ids(ydb_cluster_with_extra_sids_controls, tenant_database):
    base_url = get_mon_base_url(ydb_cluster_with_extra_sids_controls)
    return get_nodelist_ids(base_url, database=tenant_database)


@pytest.fixture(scope='module')
def foreign_node_id(ydb_cluster_with_extra_sids_controls, tenant_database):
    base_url = get_mon_base_url(ydb_cluster_with_extra_sids_controls)
    return get_foreign_node_id_for_database(base_url, tenant_database)


# Storage groups of the tenant database and the nodes/pdisks they live on.
@pytest.fixture(scope='module')
def tenant_storage_ids(ydb_cluster_with_extra_sids_controls, tenant_database):
    base_url = get_mon_base_url(ydb_cluster_with_extra_sids_controls)
    return wait_for_storage_ids(base_url, tenant_database)


# The same for the whole cluster, so that a test can pick an id outside the tenant database.
@pytest.fixture(scope='module')
def cluster_storage_ids(ydb_cluster_with_extra_sids_controls, tenant_storage_ids):
    base_url = get_mon_base_url(ydb_cluster_with_extra_sids_controls)
    return get_storage_ids(base_url)


@pytest.fixture(scope='module')
def unknown_node_id(ydb_cluster_with_extra_sids_controls):
    base_url = get_mon_base_url(ydb_cluster_with_extra_sids_controls)
    return get_unknown_node_id(base_url)


@pytest.fixture
def mon_base_url_without_extra_sids_control(ydb_cluster_without_extra_sids_controls):
    return get_mon_base_url(ydb_cluster_without_extra_sids_controls)


@pytest.fixture(scope='module')
def ydb_cluster_without_extra_sids_controls(certificates):
    configurator = create_ydb_configurator(
        certificates,
        enforce_user_token_requirement=True,
    )
    configurator.yaml_config.setdefault('feature_flags', {})['enable_extra_sids_control_for_http_viewer'] = False
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


@pytest.fixture(scope='module')
def ydb_cluster_with_secure_devui_flag_and_hive_destroy_operations(certificates):
    configurator = create_ydb_configurator(
        certificates,
        enforce_user_token_requirement=True,
        enable_tablet_dev_ui_secure_path=True,
    )
    # ResetTablet and DeleteTablet reject every request unless the Hive allows destroy operations.
    configurator.yaml_config['hive_config'] = {'enable_destroy_operations': True}
    cluster = KiKiMR(configurator)
    cluster.start()
    yield cluster
    cluster.stop()
