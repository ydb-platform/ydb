# -*- coding: utf-8 -*-
import os
import logging
import pytest
import contextlib

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure


logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def local_cluster_configuration():
    configurator = KikimrConfigGenerator(
        erasure=Erasure.NONE,
        nodes=3,
        n_to_select=1,
        additional_log_configs={
            'FLAT_TX_SCHEMESHARD': 7,
            'SCHEME_BOARD_POPULATOR': 4,
            'SCHEME_BOARD_SUBSCRIBER': 4,
            'TX_DATASHARD': 7,
            'CHANGE_EXCHANGE': 7,
        }
    )
    return configurator


@pytest.fixture(scope='module')
def ydb_cluster(local_cluster_configuration, request):
    module_name = request.module.__name__

    logger.info("setup ydb_cluster for %s", module_name)

    logger.info("setup ydb_cluster as local")
    cluster = kikimr_cluster_factory(
        configurator=local_cluster_configuration
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


@pytest.fixture(scope='module')
def ydb_endpoint(ydb_cluster):
    return "%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port)


@pytest.fixture(scope='function')
def extended_test_name(request):
    return request.node.name


@contextlib.contextmanager
def ydb_database_ctx(ydb_cluster, database, timeout_seconds=300):
    logger.info("setup ydb_database %s", database)

    ydb_cluster.remove_database(
        database,
        timeout_seconds=timeout_seconds
    )

    ydb_cluster.create_database(
        database,
        storage_pool_units_count={
            'hdd': 1
        },
        timeout_seconds=timeout_seconds
    )
    slots = ydb_cluster.register_and_start_slots(database, count=1)

    try:
        yield database
    finally:
        logger.info("destroy ydb_database for %s", database)
        for slot in slots:
            slot.stop()

        ydb_cluster.remove_database(
            database,
            timeout_seconds=timeout_seconds
        )


@pytest.fixture(scope='function')
def ydb_database(ydb_cluster, ydb_root, extended_test_name):
    database = os.path.join(ydb_root, extended_test_name.replace("[", "_").replace("]", "_"))

    with ydb_database_ctx(ydb_cluster, database):
        yield database
