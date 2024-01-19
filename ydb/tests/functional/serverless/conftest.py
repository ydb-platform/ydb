# -*- coding: utf-8 -*-
import os
import logging
import pytest
import contextlib


# XXX: setting of pytest_plugins should work if specified directly in test modules
# but somehow it does not
#
# for ydb_{cluster, database, ...} fixture family
pytest_plugins = 'ydb.tests.library.harness.ydb_fixtures'


logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def metering_file_path(ydb_configurator):
    return ydb_configurator.metering_file_path


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

    database_nodes = ydb_cluster.register_and_start_slots(database, count=3)
    ydb_cluster.wait_tenant_up(database)

    try:
        yield database
    finally:
        logger.info("destroy ydb_hostel_db for %s", database)

        ydb_cluster.remove_database(
            database,
            timeout_seconds=timeout_seconds
        )

        ydb_cluster.unregister_and_stop_slots(database_nodes)


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
def ydb_serverless_db(ydb_cluster, ydb_root, ydb_hostel_db, ydb_safe_test_name):
    database_name = os.path.join(ydb_root, "serverless", ydb_safe_test_name)

    with ydb_serverless_db_ctx(ydb_cluster, database_name, ydb_hostel_db):
        yield database_name


@pytest.fixture(scope='function')
def ydb_quoted_serverless_db(ydb_cluster, ydb_root, ydb_hostel_db, ydb_safe_test_name):
    database_name = os.path.join(ydb_root, "quoted_serverless", ydb_safe_test_name)
    schema_quotas = ((2, 60), (4, 600))

    with ydb_serverless_db_ctx(ydb_cluster, database_name, ydb_hostel_db, schema_quotas=schema_quotas):
        yield database_name


@pytest.fixture(scope='function')
def ydb_disk_quoted_serverless_db(ydb_cluster, ydb_root, ydb_hostel_db, ydb_safe_test_name):
    database_name = os.path.join(ydb_root, "quoted_serverless", ydb_safe_test_name)
    disk_quotas = {'hard': 64 * 1024 * 1024, 'soft': 32 * 1024 * 1024}

    with ydb_serverless_db_ctx(ydb_cluster, database_name, ydb_hostel_db, disk_quotas=disk_quotas):
        yield database_name


@contextlib.contextmanager
def ydb_serverless_db_with_exclusive_nodes_ctx(ydb_cluster, database, hostel_db, timeout_seconds=100):
    logger.info("setup ydb_serverless_db_with_exclusive_nodes %s using hostel %s", database, hostel_db)

    ydb_cluster.remove_database(
        database,
        timeout_seconds=timeout_seconds
    )

    ydb_cluster.create_serverless_database(
        database,
        hostel_db=hostel_db,
        timeout_seconds=timeout_seconds,
        attributes={
            "cloud_id": "CLOUD_ID_VAL",
            "folder_id": "FOLDER_ID_VAL",
            "database_id": "DATABASE_ID_VAL",
        },
    )

    database_nodes = ydb_cluster.register_and_start_slots(database, 3)
    ydb_cluster.wait_tenant_up(database)

    try:
        yield database
    finally:
        logger.info("destroy ydb_serverless_db_with_exclusive_nodes for %s", database)
        ydb_cluster.remove_database(
            database,
            timeout_seconds=timeout_seconds
        )

        ydb_cluster.unregister_and_stop_slots(database_nodes)


@pytest.fixture(scope='module')
def ydb_serverless_db_with_exclusive_nodes(ydb_cluster, ydb_root, ydb_hostel_db):
    database_name = os.path.join(ydb_root, "serverless_with_exclusive_nodes")

    with ydb_serverless_db_with_exclusive_nodes_ctx(ydb_cluster, database_name, ydb_hostel_db):
        yield database_name
