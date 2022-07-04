# -*- coding: utf-8 -*-
import os
import logging
import time

from hamcrest import (
    any_of,
    assert_that,
    calling,
    equal_to,
    raises,
)

import ydb

from common import DBWithDynamicSlot, DBForStaticSlots, Runtime

logger = logging.getLogger(__name__)


class TestCreateTenantNoCPU(DBWithDynamicSlot):
    def test_case(self):
        database = '/Root/users/database'
        self.cluster.create_database(
            database,
            storage_pool_units_count={
                'hdd': 1
            }
        )
        self.cluster.remove_database(database)


class TestCreateTenantWithCPU(DBWithDynamicSlot):
    def test_case(self):
        database = '/Root/users/database'
        self.cluster.create_database(
            database,
            storage_pool_units_count={
                'hdd': 1
            }
        )
        self.cluster.register_and_start_slots(database, count=1)
        self.cluster.wait_tenant_up(database)
        self.cluster.remove_database(database)


class TestCreateTenantThenExecYQLEmptyDatabaseHeader(DBWithDynamicSlot):
    def test_case(self):
        database = '/Root/users/database'

        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
            database
        )

        self.cluster.create_database(
            database,
            storage_pool_units_count={
                'hdd': 1
            }
        )
        self.cluster.register_and_start_slots(database, count=1)
        self.cluster.wait_tenant_up(database)

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
            with ydb.SessionPool(driver, size=1) as pool:
                with pool.checkout() as session:
                    session.execute_scheme(
                        "create table `{}` (key Int32, value String, primary key(key));".format(
                            table_path
                        )
                    )

                    session.transaction().execute(
                        "upsert into `{}` (key) values (101);".format(table_path),
                        commit_tx=True
                    )

                    session.transaction().execute("select key from `{}`;".format(table_path), commit_tx=True)


class TestCreateTenantThenExecYQL(DBWithDynamicSlot):
    def test_case(self):
        database = '/Root/users/database'

        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
            database
        )

        driver_config2 = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
            database + "/"
        )

        self.cluster.create_database(
            database,
            storage_pool_units_count={
                'hdd': 1
            }
        )
        self.cluster.register_and_start_slots(database, count=1)
        self.cluster.wait_tenant_up(database)

        d_configs = [driver_config, driver_config2]
        for d_config in d_configs:
            table_path = '%s/table-1' % database
            with ydb.Driver(d_config) as driver:
                with ydb.SessionPool(driver, size=1) as pool:
                    with pool.checkout() as session:
                        session.execute_scheme(
                            "create table `{}` (key Int32, value String, primary key(key));".format(
                                table_path
                            )
                        )

                        session.transaction().execute(
                            "upsert into `{}` (key) values (101);".format(table_path),
                            commit_tx=True
                        )

                        session.transaction().execute("select key from `{}`;".format(table_path), commit_tx=True)


class TestCreateAndDropTenants(DBWithDynamicSlot):
    def test_case(self):
        for iNo in range(10):
            database = '/Root/users/database_%d' % iNo

            driver_config = ydb.DriverConfig(
                "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
                database
            )

            self.cluster.create_database(
                database,
                storage_pool_units_count={
                    'hdd': 1
                }
            )
            self.cluster.register_and_start_slots(database, count=1)
            self.cluster.wait_tenant_up(database)

            with ydb.Driver(driver_config) as driver:
                with ydb.SessionPool(driver) as pool:
                    def create_table(session, table):
                        session.create_table(
                            os.path.join(database, table),
                            ydb.TableDescription()
                            .with_column(ydb.Column('id', ydb.OptionalType(ydb.DataType.Uint64)))
                            .with_column(ydb.Column('value', ydb.OptionalType(ydb.DataType.Utf8)))
                            .with_primary_key('id')
                        )

                    pool.retry_operation_sync(create_table, self.robust_retries, "table")
                    pool.retry_operation_sync(create_table, self.robust_retries, "table_for_rm")

                    def write_some_data(session, table_one, table_two, value):
                        session.transaction().execute(
                            "upsert into {table_one} (id, value) "
                            "values (1u, \"{val_one}\");"
                            "upsert into {table_two} (id, value) "
                            "values (2u, \"{val_two}\");"
                            "".format(table_one=table_one, val_one=value,
                                      table_two=table_two, val_two=value),
                            commit_tx=True,
                        )
                    pool.retry_operation_sync(write_some_data, self.robust_retries, "table", "table_for_rm", database)

                    def read_some_data(session, table_one, table_two):
                        result = session.transaction().execute(
                            "select id, value FROM {table_one};"
                            "select id, value FROM {table_two};"
                            "".format(table_one=table_one, table_two=table_two),
                            commit_tx=True,
                        )
                        return result

                    result = pool.retry_operation_sync(read_some_data, self.robust_retries, "table", "table_for_rm")

                    assert len(result) == 2

                    for lineNo in range(2):
                        assert_that(
                            (1 + lineNo, database),
                            equal_to(
                                (result[lineNo].rows[0].id, result[lineNo].rows[0].value)
                            )
                        )

                    def drop_table(session, table):
                        session.drop_table(
                            os.path.join(database, table)
                        )
                    pool.retry_operation_sync(drop_table, self.robust_retries, "table_for_rm")

            self.cluster.remove_database(database)


class TestCreateAndDropTheSameTenant2(DBForStaticSlots):
    def test_case(self):
        for iNo in range(4):
            database = '/Root/users/database'
            value = database + "_" + str(iNo)

            logger.debug("create_database")
            # without dynamic stots, allocate node manually as static slot
            self.cluster.create_database(
                database,
                storage_pool_units_count={
                    'hdd': 1
                }
            )

            driver_config = ydb.DriverConfig(
                "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
                database
            )

            with Runtime(self.cluster, database):
                with ydb.Driver(driver_config) as driver:
                    with ydb.SessionPool(driver, size=1) as pool:
                        def create_table(session, table):
                            session.create_table(
                                os.path.join(database, table),
                                ydb.TableDescription()
                                .with_column(ydb.Column('id', ydb.OptionalType(ydb.DataType.Uint64)))
                                .with_column(ydb.Column('value', ydb.OptionalType(ydb.DataType.Utf8)))
                                .with_primary_key('id')
                            )

                        logger.debug("create table one")
                        pool.retry_operation_sync(create_table, self.robust_retries, "table")
                        logger.debug("create table two")
                        pool.retry_operation_sync(create_table, None, "table_for_rm")

                        def write_some_data(session, table_one, table_two, value):
                            session.transaction().execute(
                                "upsert into {table_one} (id, value) "
                                "values (1u, \"{val_one}\");"
                                "upsert into {table_two} (id, value) "
                                "values (2u, \"{val_two}\");"
                                "".format(table_one=table_one, val_one=value,
                                          table_two=table_two, val_two=value),
                                commit_tx=True,
                            )
                        logger.debug("write_some_data")
                        pool.retry_operation_sync(write_some_data, None, "table", "table_for_rm", value)

                        def read_some_data(session, table_one, table_two):
                            result = session.transaction().execute(
                                "select id, value FROM {table_one};"
                                "select id, value FROM {table_two};"
                                "".format(table_one=table_one, table_two=table_two),
                                commit_tx=True,
                            )
                            return result

                        logger.debug("read_some_data")
                        result = pool.retry_operation_sync(read_some_data, None, "table", "table_for_rm")

                        assert len(result) == 2

                        for lineNo in range(2):
                            assert_that(
                                (1 + lineNo, value),
                                equal_to(
                                    (result[lineNo].rows[0].id, result[lineNo].rows[0].value)
                                )
                            )

                        def drop_table(session, table):
                            session.drop_table(
                                os.path.join(database, table)
                            )

                        logger.debug("drop table two")
                        pool.retry_operation_sync(drop_table, None, "table_for_rm")

            logger.debug("remove_database")
            self.cluster.remove_database(database)

            logger.debug("done %d", iNo)


class TestCheckAccess(DBWithDynamicSlot):
    SLOT_COUNT = 2

    def test_case(self):
        users = {}
        for user in ('user_1', 'user_2'):
            users[user] = {
                'path': os.path.join('/Root/users', user),
                'owner': '%s@builtin' % user,
            }

        for user in users.values():
            self.cluster.create_database(
                user['path'],
                storage_pool_units_count={
                    'hdd': 1
                }
            )

            self.cluster.register_and_start_slots(user['path'], count=1)
            self.cluster.wait_tenant_up(user['path'])

            driver_config = ydb.DriverConfig(
                "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
                user['path']
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
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
            user_1['path'], auth_token=user_1['owner']
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

            with ydb.SessionPool(driver, size=1) as pool:
                with pool.checkout() as session:
                    session.execute_scheme(
                        "create table `{}` (id Int64, primary key(id));".format(
                            os.path.join(user_1['path'], 'q/w/table')
                        )
                    )
                    assert_that(
                        calling(session.execute_scheme).with_args(
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
