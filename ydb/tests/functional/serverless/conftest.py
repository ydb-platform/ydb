# -*- coding: utf-8 -*-
import os
import logging
import pytest
import contextlib

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory 
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator 
from ydb.tests.library.harness.util import LogLevels 
from ydb.tests.library.common.types import Erasure 


logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def local_cluster_configuration():
    configurator = KikimrConfigGenerator(
        erasure=Erasure.NONE,
        nodes=1,
        enable_metering=True,
        disable_mvcc=True,
        additional_log_configs={
            'TX_PROXY': LogLevels.DEBUG,
            'KQP_PROXY': LogLevels.DEBUG,
            'KQP_WORKER': LogLevels.DEBUG,
            'KQP_GATEWAY': LogLevels.DEBUG,
            'GRPC_PROXY': LogLevels.TRACE,
            'KQP_YQL': LogLevels.DEBUG,
            'TX_DATASHARD': LogLevels.DEBUG,
            'FLAT_TX_SCHEMESHARD': LogLevels.DEBUG,
            'SCHEMESHARD_DESCRIBE': LogLevels.DEBUG,

            'SCHEME_BOARD_POPULATOR': LogLevels.DEBUG,

            'SCHEME_BOARD_REPLICA': LogLevels.ERROR,
            'SCHEME_BOARD_SUBSCRIBER': LogLevels.ERROR,
            'TX_PROXY_SCHEME_CACHE': LogLevels.ERROR,

            'CMS': LogLevels.DEBUG,
            'CMS_TENANTS': LogLevels.DEBUG,

        }
    )
    return configurator


@pytest.fixture(scope='module')
def metering_file_path(local_cluster_configuration):
    return local_cluster_configuration.metering_file_path


@pytest.fixture(scope='module')
def ydb_cluster(local_cluster_configuration, request):
    module_name = request.module.__name__

    logger.info("setup ydb_cluster for %s", module_name)

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
def ydb_database_ctx(ydb_cluster, database, timeout_seconds=100):
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
    ydb_cluster.wait_tenant_up(database) 
 
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


@contextlib.contextmanager
def ydb_hostel_db_ctx(ydb_cluster, ydb_root, timeout_seconds=100):
    database = os.path.join(ydb_root, "hostel_db")
    logger.info("setup ydb_hostel_db %s", database)

    ydb_cluster.remove_database(
        database,
        timeout_seconds=timeout_seconds
    )

    ydb_cluster.create_hostel_database(
        database,
        storage_pool_units_count={
            'hdd': 1
        },
        timeout_seconds=timeout_seconds
    )

    slots = ydb_cluster.register_and_start_slots(database, count=3) 
    ydb_cluster.wait_tenant_up(database) 
 
    try:
        yield database
    finally:
        logger.info("destroy ydb_hostel_db for %s", database)
        for slot in slots: 
            slot.stop() 

        ydb_cluster.remove_database(
            database,
            timeout_seconds=timeout_seconds
        )


@pytest.fixture(scope='module')
def ydb_hostel_db(ydb_cluster, ydb_root):
    with ydb_hostel_db_ctx(ydb_cluster, ydb_root) as db_name:
        yield db_name


@contextlib.contextmanager
def ydb_serverless_db_ctx(ydb_cluster, database, hostel_db, timeout_seconds=100, schema_quotas=None, disk_quotas=None):
    logger.info("setup ydb_serverless_db %s over hostel %s with schema_quotas=%r, disk_quotas=%r", database, hostel_db, schema_quotas, disk_quotas)

    ydb_cluster.remove_database(
        database,
        timeout_seconds=timeout_seconds
    )

    ydb_cluster.create_serverless_database(
        database,
        hostel_db=hostel_db,
        timeout_seconds=timeout_seconds,
        schema_quotas=schema_quotas,
        disk_quotas=disk_quotas,
        attributes={
            "cloud_id": "CLOUD_ID_VAL",
            "folder_id": "FOLDER_ID_VAL",
            "database_id": "DATABASE_ID_VAL",
        },
    )

    try:
        yield database
    finally:
        logger.info("destroy ydb_serverless_db for %s", database)
        ydb_cluster.remove_database(
            database,
            timeout_seconds=timeout_seconds
        )


@pytest.fixture(scope='function')
def ydb_serverless_db(ydb_cluster, ydb_root, ydb_hostel_db, extended_test_name):
    database_name = os.path.join(ydb_root, "serverless", extended_test_name.replace("[", "_").replace("]", "_"))

    with ydb_serverless_db_ctx(ydb_cluster, database_name, ydb_hostel_db):
        yield database_name


@pytest.fixture(scope='function')
def ydb_quoted_serverless_db(ydb_cluster, ydb_root, ydb_hostel_db, extended_test_name):
    database_name = os.path.join(ydb_root, "quoted_serverless", extended_test_name.replace("[", "_").replace("]", "_"))
    schema_quotas = ((2, 60), (4, 600))

    with ydb_serverless_db_ctx(ydb_cluster, database_name, ydb_hostel_db, schema_quotas=schema_quotas):
        yield database_name


@pytest.fixture(scope='function')
def ydb_disk_quoted_serverless_db(ydb_cluster, ydb_root, ydb_hostel_db, extended_test_name):
    database_name = os.path.join(ydb_root, "quoted_serverless", extended_test_name.replace("[", "_").replace("]", "_"))
    disk_quotas = {'hard': 64 * 1024 * 1024, 'soft': 32 * 1024 * 1024}

    with ydb_serverless_db_ctx(ydb_cluster, database_name, ydb_hostel_db, disk_quotas=disk_quotas):
        yield database_name
