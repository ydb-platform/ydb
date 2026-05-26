# -*- coding: utf-8 -*-
import time

import pytest
import requests

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb

from cluster_config import create_ydb_configurator, generate_certificates

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


@pytest.fixture(scope='module')
def ydb_cluster_with_enforce_user_token_and_pers_queue_topic(certificates):
    configurator = create_ydb_configurator(
        certificates,
        enforce_user_token_requirement=True,
    )
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
