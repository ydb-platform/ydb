# -*- coding: utf-8 -*-
import contextlib
import logging
import os

import pytest

from ydb import Driver, DriverConfig, SessionPool

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels

logger = logging.getLogger(__name__)


DEFAULT_CLUSTER_CONFIG = dict(
    erasure=Erasure.NONE,
    nodes=1,
    additional_log_configs={
        'FLAT_TX_SCHEMESHARD': LogLevels.DEBUG,
        'SCHEME_BOARD_POPULATOR': LogLevels.WARN,
        'SCHEME_BOARD_SUBSCRIBER': LogLevels.WARN,
        'TX_DATASHARD': LogLevels.DEBUG,
        'CHANGE_EXCHANGE': LogLevels.DEBUG,
    },
)


@pytest.fixture(scope='module')
def ydb_cluster_configuration(request):
    conf = getattr(request.module, 'CLUSTER_CONFIG', DEFAULT_CLUSTER_CONFIG)
    return conf


@pytest.fixture(scope='module')
def ydb_configurator(ydb_cluster_configuration):
    return KikimrConfigGenerator(**ydb_cluster_configuration)


@pytest.fixture(scope='module')
def ydb_cluster(ydb_configurator, request):
    module_name = request.module.__name__

    logger.info("setup ydb_cluster for %s", module_name)

    logger.info("setup ydb_cluster as local")
    cluster = kikimr_cluster_factory(
        configurator=ydb_configurator,
    )
    cluster.is_local_test = True

    cluster.start()

    yield cluster

    logger.info("destroy ydb_cluster for %s", module_name)
    cluster.stop()


@pytest.fixture(scope='module')
def ydb_root(ydb_cluster):
    return os.path.join("/", ydb_cluster.domain_name)


@pytest.fixture(scope='module')
def ydb_private_client(ydb_cluster):
    return ydb_cluster.client


@pytest.fixture(scope='function')
def ydb_safe_test_name(request):
    return request.node.name.replace("[", "_").replace("]", "_")


@contextlib.contextmanager
def ydb_database_ctx(ydb_cluster, database_path, node_count=1, timeout_seconds=20, storage_pools={'hdd': 1}):
    '''???'''
    assert os.path.abspath(database_path), 'database_path should be an (absolute) path, not a database name'

    ydb_cluster.remove_database(database_path, timeout_seconds=timeout_seconds)

    logger.debug("create database %s: create path and declare internals", database_path)

    ydb_cluster.create_database(database_path, storage_pool_units_count=storage_pools, timeout_seconds=timeout_seconds)

    logger.debug("create database %s: start nodes and construct internals", database_path)
    database_nodes = ydb_cluster.register_and_start_slots(database_path, node_count)

    logger.debug("create database %s: wait construction done", database_path)
    ydb_cluster.wait_tenant_up(database_path)

    logger.debug("create database %s: database up", database_path)
    yield database_path

    logger.debug("destroy database %s: remove path and dismantle internals", database_path)
    ydb_cluster.remove_database(database_path, timeout_seconds=timeout_seconds)

    logger.debug("destroy database %s: stop nodes", database_path)
    ydb_cluster.unregister_and_stop_slots(database_nodes)

    logger.debug("destroy database %s: database down", database_path)


@pytest.fixture(scope='function')
def ydb_database(ydb_cluster, ydb_root, ydb_safe_test_name):
    database = os.path.join(ydb_root, ydb_safe_test_name)

    with ydb_database_ctx(ydb_cluster, database):
        yield database


@pytest.fixture(scope='function')
def ydb_endpoint(ydb_cluster):
    return "%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port)


@pytest.fixture(scope='function')
def ydb_client(ydb_endpoint, request):
    def _make_driver(database_path, **kwargs):
        driver_config = DriverConfig(ydb_endpoint, database_path, **kwargs)
        driver = Driver(driver_config)

        def stop_driver():
            driver.stop()

        request.addfinalizer(stop_driver)
        return driver

    return _make_driver


@pytest.fixture(scope='function')
def ydb_client_session(ydb_client, request):
    def _make_pool(database_path, **kwargs):
        driver = ydb_client(database_path, **kwargs)
        pool = SessionPool(driver)

        def stop_pool():
            pool.stop()

        request.addfinalizer(stop_pool)
        return pool

    return _make_pool


# possible replacement for both ydb_client and ydb_client_session
# @pytest.fixture(scope='function')
# def ydb_database_and_client(ydb_database, ydb_endpoint):
#     database_path = ydb_database
#     with Driver(DriverConfig(ydb_endpoint, database_path)) as driver:
#         with SessionPool(driver) as pool:
#             yield database_path, pool
