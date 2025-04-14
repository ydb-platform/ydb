# -*- coding: utf-8 -*-
import os
import logging
import time
import copy
import pytest

from hamcrest import (
    any_of,
    assert_that,
    calling,
    equal_to,
    raises,
)

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.harness.util import LogLevels


logger = logging.getLogger(__name__)


# local configuration for the ydb cluster (fetched by ydb_cluster_configuration fixture)
CLUSTER_CONFIG = dict(
    additional_log_configs={
        'TX_PROXY': LogLevels.DEBUG,
        'KQP_PROXY': LogLevels.DEBUG,
        'KQP_WORKER': LogLevels.DEBUG,
        'KQP_GATEWAY': LogLevels.DEBUG,
        'GRPC_PROXY': LogLevels.TRACE,
        'TX_DATASHARD': LogLevels.DEBUG,
        'TX_PROXY_SCHEME_CACHE': LogLevels.DEBUG,
        'GRPC_SERVER': LogLevels.DEBUG,
        # more logs
        'FLAT_TX_SCHEMESHARD': LogLevels.TRACE,
        'HIVE': LogLevels.TRACE,
        'CMS_TENANTS': LogLevels.TRACE,
        # less logs
        'KQP_YQL': LogLevels.ERROR,
        'KQP_SESSION': LogLevels.CRIT,
        'KQP_COMPILE_ACTOR': LogLevels.CRIT,
        'PERSQUEUE_CLUSTER_TRACKER': LogLevels.CRIT,
    },
    enable_alter_database_create_hive_first=True,
)


@pytest.fixture(scope='module', params=[True, False], ids=['enable_alter_database_create_hive_first--true', 'enable_alter_database_create_hive_first--false'])
def enable_alter_database_create_hive_first(request):
    return request.param


# fixtures.ydb_cluster_configuration local override
@pytest.fixture(scope='module')
def ydb_cluster_configuration(enable_alter_database_create_hive_first):
    conf = copy.deepcopy(CLUSTER_CONFIG)
    conf['enable_alter_database_create_hive_first'] = enable_alter_database_create_hive_first
    return conf


def test_create_tenant_no_cpu(ydb_cluster):
    database = '/Root/users/database'
    ydb_cluster.create_database(
        database,
        storage_pool_units_count={
            'hdd': 1
        }
    )
    ydb_cluster.remove_database(database)


def test_create_tenant_with_cpu(ydb_cluster):
    database = '/Root/users/database'
    ydb_cluster.create_database(
        database,
        storage_pool_units_count={
            'hdd': 1
        }
    )
    database_nodes = ydb_cluster.register_and_start_slots(database, count=1)
    ydb_cluster.wait_tenant_up(database)
    time.sleep(1)
    ydb_cluster.remove_database(database)
    ydb_cluster.unregister_and_stop_slots(database_nodes)


def test_drop_tenant_without_nodes_could_continue(ydb_cluster):
    database = '/Root/users/database'
    ydb_cluster.create_database(
        database,
        storage_pool_units_count={
            'hdd': 1
        }
    )
    database_nodes = ydb_cluster.register_and_start_slots(database, count=1)
    ydb_cluster.wait_tenant_up(database)
    time.sleep(1)

    logger.debug("stop database nodes")
    ydb_cluster.unregister_and_stop_slots(database_nodes)

    logger.debug("remove database")
    operation_id = ydb_cluster._remove_database_send_op(database)

    logger.debug("restart database nodes")
    database_nodes = ydb_cluster.register_and_start_slots(database, count=1)

    ydb_cluster._remove_database_wait_op(database, operation_id)
    ydb_cluster._remove_database_wait_tenant_gone(database)

    ydb_cluster.unregister_and_stop_slots(database_nodes)


def test_drop_tenant_without_nodes_could_complete(ydb_cluster):
    database = '/Root/users/database'
    ydb_cluster.create_database(
        database,
        storage_pool_units_count={
            'hdd': 1
        }
    )
    database_nodes = ydb_cluster.register_and_start_slots(database, count=1)
    ydb_cluster.wait_tenant_up(database)
    time.sleep(1)

    logger.debug("stop database nodes")
    ydb_cluster.unregister_and_stop_slots(database_nodes)

    logger.debug("remove database")
    ydb_cluster.remove_database(database)


def test_create_tenant_then_exec_yql_empty_database_header(ydb_cluster, ydb_endpoint):
    database = '/Root/users/database'

    driver_config = ydb.DriverConfig(ydb_endpoint, database)

    ydb_cluster.create_database(
        database,
        storage_pool_units_count={
            'hdd': 1
        }
    )
    database_nodes = ydb_cluster.register_and_start_slots(database, count=1)
    ydb_cluster.wait_tenant_up(database)

    def list_endpoints(database):
        logger.debug("List endpoints of %s", database)
        resolver = ydb.DiscoveryEndpointsResolver(driver_config)
        result = resolver.resolve()
        if result is not None:
            return result.endpoints
        return result

    endpoints = list_endpoints(database)

    driver_config2 = ydb.DriverConfig(
        "%s" % endpoints[0].endpoint,
        None,
        credentials=ydb.AuthTokenCredentials("root@builtin")
    )

    table_path = '%s/table-1' % database
    with ydb.Driver(driver_config2) as driver:
        with ydb.QuerySessionPool(driver, size=1) as pool:
            pool.execute_with_retries(
                "create table `{}` (key Int32, value String, primary key(key));".format(
                    table_path
                ),
            )

            pool.execute_with_retries("upsert into `{}` (key) values (101);".format(table_path))

            pool.execute_with_retries("select key from `{}`;".format(table_path))

    ydb_cluster.remove_database(database)
    ydb_cluster.unregister_and_stop_slots(database_nodes)


def test_create_tenant_then_exec_yql(ydb_cluster):
    database = '/Root/users/database'

    driver_config = ydb.DriverConfig(
        "%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database
    )

    ydb_cluster.create_database(
        database,
        storage_pool_units_count={
            'hdd': 1
        }
    )
    database_nodes = ydb_cluster.register_and_start_slots(database, count=1)
    ydb_cluster.wait_tenant_up(database)

    table_path = '%s/table-1' % database
    with ydb.Driver(driver_config) as driver:
        with ydb.QuerySessionPool(driver, size=1) as pool:
            pool.execute_with_retries(
                "create table `{}` (key Int32, value String, primary key(key));".format(
                    table_path
                )
            )

            pool.execute_with_retries("upsert into `{}` (key) values (101);".format(table_path),)

            pool.execute_with_retries("select key from `{}`;".format(table_path))

    ydb_cluster.remove_database(database)
    ydb_cluster.unregister_and_stop_slots(database_nodes)


def test_create_and_drop_tenants(ydb_cluster, robust_retries):
    for iNo in range(10):
        database = '/Root/users/database_%d' % iNo

        driver_config = ydb.DriverConfig(
            "%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
            database
        )

        ydb_cluster.create_database(
            database,
            storage_pool_units_count={
                'hdd': 1
            }
        )
        database_nodes = ydb_cluster.register_and_start_slots(database, count=1)
        ydb_cluster.wait_tenant_up(database)

        with ydb.Driver(driver_config) as driver:
            with ydb.QuerySessionPool(driver) as pool:
                def create_table(pool, table, retry_settings):
                    pool.execute_with_retries(
                        f"""
                        CREATE TABLE `{os.path.join(database, table)}`
                        (
                        id Uint64,
                        value Utf8,
                        PRIMARY KEY (id)
                        );
                        """,
                        retry_settings=retry_settings,
                    )

                create_table(pool, "table", robust_retries)
                create_table(pool, "table_for_rm", robust_retries)

                def write_some_data(pool, table_one, table_two, value, retry_settings):
                    pool.execute_with_retries(
                        f"""
                        upsert into {table_one} (id, value)
                        values (1u, "{value}");
                        upsert into {table_two} (id, value)
                        values (2u, "{value}");
                        """,
                        retry_settings=retry_settings,
                    )
                write_some_data(pool, "table", "table_for_rm", database, robust_retries)

                def read_some_data(pool, table_one, table_two, retry_settings):
                    result = pool.execute_with_retries(
                        f"""
                        select id, value FROM {table_one};
                        select id, value FROM {table_two};
                        """,
                        retry_settings=retry_settings,
                    )
                    return result

                result = read_some_data(pool, "table", "table_for_rm", robust_retries)

                assert len(result) == 2

                for lineNo in range(2):
                    assert_that(
                        (1 + lineNo, database),
                        equal_to(
                            (result[lineNo].rows[0].id, result[lineNo].rows[0].value)
                        )
                    )

                def drop_table(pool, table, retry_settings):
                    pool.execute_with_retries(
                        f"DROP TABLE `{os.path.join(database, table)}`",
                        retry_settings=retry_settings,
                    )
                drop_table(pool, "table_for_rm", robust_retries)

        ydb_cluster.remove_database(database)
        ydb_cluster.unregister_and_stop_slots(database_nodes)


def test_create_and_drop_the_same_tenant2(ydb_cluster, ydb_endpoint, robust_retries):
    for iNo in range(4):
        database = '/Root/users/database'
        value = database + "_" + str(iNo)

        logger.debug("create_database")
        # without dynamic stots, allocate node manually as static slot
        ydb_cluster.create_database(
            database,
            storage_pool_units_count={
                'hdd': 1
            }
        )

        driver_config = ydb.DriverConfig(ydb_endpoint, database)

        database_nodes = ydb_cluster.register_and_start_slots(database, count=1)

        with ydb.Driver(driver_config) as driver:
            with ydb.QuerySessionPool(driver, size=1) as pool:
                def create_table(pool, table, retry_settings):
                    pool.execute_with_retries(
                        f"""
                        CREATE TABLE `{os.path.join(database, table)}`
                        (
                        id Uint64,
                        value Utf8,
                        PRIMARY KEY (id)
                        );
                        """,
                        retry_settings=retry_settings,
                    )

                logger.debug("create table one")
                create_table(pool, "table", robust_retries)
                logger.debug("create table two")
                create_table(pool, "table_for_rm", None)

                def write_some_data(pool, table_one, table_two, value, retry_settings):
                    pool.execute_with_retries(
                        f"""
                        upsert into {table_one} (id, value)
                        values (1u, "{value}");
                        upsert into {table_two} (id, value)
                        values (2u, "{value}");
                        """,
                        retry_settings=retry_settings,
                    )

                logger.debug("write_some_data")
                write_some_data(pool, "table", "table_for_rm", value, None)

                def read_some_data(pool, table_one, table_two, retry_settings):
                    result = pool.execute_with_retries(
                        f"""
                        select id, value FROM {table_one};
                        select id, value FROM {table_two};
                        """,
                        retry_settings=retry_settings,
                    )
                    return result

                logger.debug("read_some_data")
                result = read_some_data(pool, "table", "table_for_rm", None)

                assert len(result) == 2

                for lineNo in range(2):
                    assert_that(
                        (1 + lineNo, value),
                        equal_to(
                            (result[lineNo].rows[0].id, result[lineNo].rows[0].value)
                        )
                    )

                def drop_table(pool, table, retry_settings):
                    pool.execute_with_retries(
                        f"DROP TABLE {os.path.join(database, table)}",
                        retry_settings=retry_settings,
                    )

                logger.debug("drop table two")
                drop_table(pool, "table_for_rm", None)

        logger.debug("remove_database")
        ydb_cluster.remove_database(database)
        ydb_cluster.unregister_and_stop_slots(database_nodes)

        logger.debug("done %d", iNo)


def test_check_access(ydb_cluster):
    users = {}
    for user in ('user_1', 'user_2'):
        users[user] = {
            'path': os.path.join('/Root/users', user),
            'owner': '%s@builtin' % user,
        }

    database_nodes = {}

    for user in users.values():
        ydb_cluster.create_database(
            user['path'],
            storage_pool_units_count={
                'hdd': 1
            }
        )

        database_nodes[user['path']] = ydb_cluster.register_and_start_slots(user['path'], count=1)
        ydb_cluster.wait_tenant_up(user['path'])

        driver_config = ydb.DriverConfig(
            database=user['path'],
            endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        )

        with ydb.Driver(driver_config) as driver:
            driver.wait(timeout=10)

            client = ydb.SchemeClient(driver)
            client.modify_permissions(
                user['path'],
                ydb.ModifyPermissionsSettings().change_owner(user['owner'])
            )

    user_1 = users['user_1']
    user_2 = users['user_2']

    driver_config = ydb.DriverConfig(
        database=user_1['path'],
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        auth_token=user_1['owner'],
    )

    with ydb.Driver(driver_config) as driver:
        driver.wait(timeout=10)
        client = ydb.SchemeClient(driver)

        while True:
            try:
                client.list_directory(user_1['path'])
            except ydb.Unauthorized:
                time.sleep(5)  # wait until caches are refreshed
            else:
                break

        assert_that(
            calling(client.list_directory).with_args(
                user_2['path']
            ),
            raises(ydb.Unauthorized)
        )

        assert_that(
            calling(client.list_directory).with_args(
                os.path.join(user_1['path'], 'a')
            ),
            raises(ydb.SchemeError)
        )
        assert_that(
            calling(client.list_directory).with_args(
                os.path.join(user_2['path'], 'a')
            ),
            raises(ydb.SchemeError)
        )

        client.make_directory(os.path.join(user_1['path'], 'a'))
        assert_that(
            calling(client.make_directory).with_args(
                os.path.join(user_2['path'], 'a')
            ),
            raises(ydb.BadRequest)
        )

        with ydb.QuerySessionPool(driver, size=1) as pool:
            pool.execute_with_retries(
                "create table `{}` (id Int64, primary key(id));".format(
                    os.path.join(user_1['path'], 'q/w/table')
                )
            )
            assert_that(
                calling(pool.execute_with_retries).with_args(
                    "create table `{}` (id Int64, primary key(id));".format(
                        os.path.join(user_2['path'], 'q/w/table')
                    )
                ),
                any_of(raises(ydb.GenericError), raises(ydb.Unauthorized))
            )

        assert_that(
            calling(client.list_directory).with_args(
                '/Root/'
            ),
            raises(ydb.SchemeError)
        )
        client.list_directory('/')

    for user in users.values():
        ydb_cluster.remove_database(user['path'])
        ydb_cluster.unregister_and_stop_slots(database_nodes[user['path']])


def test_custom_coordinator_options(ydb_cluster):
    database = '/Root/users/custom_options'
    ydb_cluster.create_database(
        database,
        storage_pool_units_count={
            'hdd': 1
        },
        options={
            'coordinators': 4,
            'mediators': 5,
            'plan_resolution': 100,
        },
    )
    database_nodes = ydb_cluster.register_and_start_slots(database, count=1)
    ydb_cluster.wait_tenant_up(database)

    description = ydb_cluster.client.describe(database, '')
    params = description.PathDescription.DomainDescription.ProcessingParams
    assert_that(
        [
            len(params.Coordinators),
            len(params.Mediators),
            params.PlanResolution,
        ],
        equal_to([4, 5, 100]))

    ydb_cluster.remove_database(database)
    ydb_cluster.unregister_and_stop_slots(database_nodes)
