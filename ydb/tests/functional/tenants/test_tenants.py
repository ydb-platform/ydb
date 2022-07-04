# -*- coding: utf-8 -*-
import os
import logging
import random
import pytest

from hamcrest import assert_that, greater_than, is_, not_, none

import ydb

from common import Runtime, DBForStaticSlots


logger = logging.getLogger(__name__)


class TestTenants(DBForStaticSlots):
    def test_when_deactivate_fat_tenant_creation_another_tenant_is_ok(self):
        logger.info("create fat tenant")
        databases = [os.path.join(self.root_dir, "database_%d") % idx for idx in range(3)]
        for database in databases:
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
                    with ydb.SessionPool(driver) as pool:
                        def create_table(session):
                            session.create_table(
                                os.path.join(database, 'table_0'),
                                ydb.TableDescription()
                                .with_column(ydb.Column('id', ydb.OptionalType(ydb.DataType.Uint64)))
                                .with_primary_key('id')
                                .with_profile(
                                    ydb.TableProfile()
                                    .with_partitioning_policy(
                                        ydb.PartitioningPolicy().with_uniform_partitions(self.boot_batch_size * 2)
                                    )
                                )
                            )
                        pool.retry_operation_sync(create_table, self.robust_retries)

                        def describe_table(session):
                            result = session.describe_table(os.path.join(database, 'table_0'))
                            logger.debug("> describe table: series, %s", str(result))
                            return result
                        pool.retry_operation_sync(describe_table)

        logger.info("remove tenants")
        for database in databases:
            self.cluster.remove_database(database)

    def test_register_tenant_and_force_drop_with_table(self):
        with Runtime(self.cluster, self.database_name, 1):
            with ydb.Driver(self.driver_config) as driver:
                with ydb.SessionPool(driver) as pool:
                    def create_table(session):
                        session.create_table(
                            os.path.join(self.database_name, 'table_0'),
                            ydb.TableDescription()
                            .with_column(ydb.Column('id', ydb.OptionalType(ydb.DataType.Uint64)))
                            .with_primary_key('id')
                        )

                    pool.retry_operation_sync(create_table, self.robust_retries)

    def test_force_delete_tenant_when_table_has_been_deleted(self):
        with Runtime(self.cluster, self.database_name):
            with ydb.Driver(self.driver_config) as driver:
                with ydb.SessionPool(driver) as pool:
                    def create_table(session_):
                        session_.create_table(
                            os.path.join(self.database_name, 'table_0'),
                            ydb.TableDescription()
                            .with_column(ydb.Column('id', ydb.OptionalType(ydb.DataType.Uint64)))
                            .with_primary_key('id')
                        )
                    pool.retry_operation_sync(create_table, self.robust_retries)

                    def describe_table(session_):
                        result = session_.describe_table(os.path.join(self.database_name, 'table_0'))
                        logger.debug("> describe table: series, %s", str(result))
                        return result
                    pool.retry_operation_sync(describe_table)

                    with pool.checkout() as session:
                        session.drop_table(os.path.join(self.database_name, 'table_0'))

    def test_progress_when_tenant_tablets_run_on_dynamic_nodes(self):
        with Runtime(self.cluster, self.database_name):
            with ydb.Driver(self.driver_config) as driver:
                with ydb.SessionPool(driver) as pool:
                    def create_table(session):
                        session.create_table(
                            os.path.join(self.database_name, 'table_0'),
                            ydb.TableDescription()
                            .with_column(ydb.Column('id', ydb.OptionalType(ydb.DataType.Uint64)))
                            .with_primary_key('id')
                        )
                    pool.retry_operation_sync(create_table, self.robust_retries)

                    def describe_table(session):
                        result = session.describe_table(os.path.join(self.database_name, 'table_0'))
                        logger.debug("> describe table: series, %s", str(result))
                        return result
                    pool.retry_operation_sync(describe_table)

                    def drop_table(session):
                        result = session.drop_table(os.path.join(self.database_name, 'table_0'))
                        logger.debug("> drop table: series, %s", str(result))
                        return result
                    pool.retry_operation_sync(drop_table)

    def test_yql_operations_over_dynamic_nodes(self):
        with Runtime(self.cluster, self.database_name):
            table_path_1 = os.path.join(self.database_name, 'table_1')
            table_path_2 = os.path.join(self.database_name, 'table_2')

            with ydb.Driver(self.driver_config) as driver:
                with ydb.SessionPool(driver) as pool:
                    def create_table(session):
                        session.execute_scheme(
                            "create table `{first}` (key Int32, value String, primary key(key));"
                            "create table `{second}` (key Int32, value String, primary key(key));"
                            "".format(
                                first=table_path_1,
                                second=table_path_2
                            )
                        )
                    pool.retry_operation_sync(create_table, self.robust_retries)

                    def upsert(session):
                        session.transaction().execute(
                            "upsert into `{first}` (key) values (101);"
                            "upsert into `{second}` (key) values (102);"
                            "".format(
                                first=table_path_1,
                                second=table_path_2
                            ),
                            commit_tx=True
                        )
                    pool.retry_operation_sync(upsert)

                    def select(session):
                        session.transaction().execute(
                            "select key from `{first}`;"
                            "select key from `{second}`;"
                            "".format(
                                first=table_path_1,
                                second=table_path_2
                            ),
                            commit_tx=True
                        )
                    pool.retry_operation_sync(select)

    def test_resolve_nodes(self):
        driver = ydb.Driver(self.driver_config)

        try:
            driver.wait(self.robust_retries.get_session_client_timeout)
        except Exception as e:
            logger.info("failed to find endpoints as expected: " + str(e))
            errors = driver.discovery_debug_details()
            logger.info("discovery details: " + str(errors))
        else:
            logger.exception("endpoints have found, not expected")
            assert False

        with Runtime(self.cluster, self.database_name):
            with ydb.Driver(self.driver_config) as driver:
                with ydb.SessionPool(driver, size=1) as pool:
                    def callee(session):
                        session.execute_scheme(
                            "CREATE TABLE wormUp (id utf8, PRIMARY KEY (id));"
                        )
                    pool.retry_operation_sync(callee, self.robust_retries)

    def test_create_tables(self):
        with Runtime(self.cluster, self.database_name):
            with ydb.Driver(self.driver_config) as driver:
                with ydb.SessionPool(driver) as pool:

                    def wormUp(session):
                        session.execute_scheme(
                            "CREATE TABLE wormUp (id utf8, PRIMARY KEY (id));"
                        )

                    pool.retry_operation_sync(wormUp, self.robust_retries)

                    create_futures = []
                    table = os.path.join(self.database_name, "temp/hardware/default/compute_az", "allocations")
                    sessions = [pool.acquire() for _ in range(10)]

                    for session in sessions:
                        create_futures.append(
                            session.async_execute_scheme(
                                "CREATE TABLE `{table}` (id utf8, PRIMARY KEY (id));".format(
                                    table=table
                                )
                            )
                        )

                    for session in sessions:
                        pool.release(session)

                    success_responses_count = 0
                    for create_future in create_futures:
                        try:
                            create_future.result()
                            success_responses_count += 1
                        except ydb.Overloaded:
                            pass
                        except ydb.Unavailable as e:
                            logger.info("ydb.Unavailable: " + str(e))

                    with pool.checkout() as session:
                        assert_that(
                            session.describe_table(table),
                            is_(
                                not_(
                                    none()
                                )
                            )
                        )

                assert_that(
                    success_responses_count,
                    greater_than(0)
                )

    def test_stop_start(self):
        def create_table(session, table):
            session.execute_scheme(
                "CREATE TABLE `{table}` (id utf8, PRIMARY KEY (id));".format(table=table)
            )

        for iNo in range(5):
            with Runtime(self.cluster, self.database_name):
                with ydb.Driver(self.driver_config) as driver:
                    with ydb.SessionPool(driver) as pool:
                        pool.retry_operation_sync(
                            create_table,
                            self.robust_retries,
                            "table_%d" % iNo)

    def test_create_drop_create_table(self):
        with Runtime(self.cluster, self.database_name, 1):
            with ydb.Driver(self.driver_config) as driver:
                with ydb.SessionPool(driver) as pool:
                    def create_table(session):
                        session.create_table(
                            os.path.join(self.database_name, 'table_0'),
                            ydb.TableDescription()
                            .with_column(ydb.Column('id', ydb.OptionalType(ydb.DataType.Uint64)))
                            .with_primary_key('id')
                        )

                    def drop_table(session):
                        session.drop_table(
                            os.path.join(self.database_name, 'table_0'),
                        )

                    pool.retry_operation_sync(create_table, self.robust_retries)
                    pool.retry_operation_sync(drop_table, self.robust_retries)
                    pool.retry_operation_sync(create_table, self.robust_retries)

    def test_create_drop_create_table2(self):
        with Runtime(self.cluster, self.database_name, 1):
            with ydb.Driver(self.driver_config) as driver:
                with ydb.SessionPool(driver) as pool:
                    def create_drop_create_table(session):
                        session.create_table(
                            os.path.join(self.database_name, 'table_0'),
                            ydb.TableDescription()
                            .with_column(ydb.Column('id', ydb.OptionalType(ydb.DataType.Uint64)))
                            .with_primary_key('id')
                        )
                        session.drop_table(
                            os.path.join(self.database_name, 'table_0'),
                        )
                        session.create_table(
                            os.path.join(self.database_name, 'table_0'),
                            ydb.TableDescription()
                            .with_column(ydb.Column('id_1', ydb.OptionalType(ydb.DataType.Uint64)))
                            .with_primary_key('id_1')
                        )

                    pool.retry_operation_sync(create_drop_create_table, self.robust_retries)

    @pytest.mark.xfail
    def test_create_drop_create_table3(self):
        with Runtime(self.cluster, self.database_name, 1):
            with ydb.Driver(self.driver_config) as driver:
                with ydb.SessionPool(driver) as pool:
                    def create_table(session):
                        session.execute_scheme('''
                            CREATE TABLE `{table}`
                            (
                                id Int64,
                                primary key (id)
                            );
                            '''.format(table=os.path.join(self.database_name, 'table_0')))
                    pool.retry_operation_sync(create_table, self.robust_retries)

                    def drop_create_table(session):
                        session.execute_scheme('''
                            DROP TABLE `{table}`;
                            CREATE TABLE `{table}`
                            (
                                id_1 Int64,
                                primary key (id_1)
                            );
                            '''.format(table=os.path.join(self.database_name, 'table_0')))

                    pool.retry_operation_sync(drop_create_table, self.robust_retries)

    def test_create_create_table(self):
        with Runtime(self.cluster, self.database_name, 1):
            with ydb.Driver(self.driver_config) as driver:
                with ydb.SessionPool(driver) as pool:
                    def create_tables(session, table_base):
                        session.execute_scheme('''
                            CREATE TABLE `{table}_0`
                            (
                                id Int64,
                                primary key (id)
                            );
                            CREATE TABLE `{table}_1`
                            (
                                id Int64,
                                primary key (id)
                            );
                            '''.format(table=os.path.join(self.database_name, table_base)))

                    pool.retry_operation_sync(create_tables, self.robust_retries, "first_two")
                    pool.retry_operation_sync(create_tables, self.robust_retries, "second_two")

    def test_list_database_above(self):
        database_path = self.database_name
        above_database, basename = os.path.split(database_path)

        def convert(item):
            def _get_entry_schema(entry):
                return ("name", os.path.basename(entry.name)), \
                       ("type", str(entry.type)), \
                       ("owner", entry.owner), \
                       ("effective_permissions", [x.to_pb() for x in entry.effective_permissions]), \
                       ("permissions", [x.to_pb() for x in entry.permissions])

            if type(item) == ydb.scheme.Directory:
                return ("Directory",) + _get_entry_schema(item) + (("children", [convert(x) for x in item.children]),)

            if type(item) == ydb.scheme.SchemeEntry:
                return ("SchemeEntry",) + _get_entry_schema(item)

            return ("UnknownEntry", type(item)) + _get_entry_schema(item)

        with Runtime(self.cluster, database_path, 1):
            with ydb.Driver(self.driver_config) as driver:
                driver.wait()

                result = driver.scheme_client.list_directory(database_path)
                logger.debug("From database: list database <%s> is %s", database_path, convert(result))
                assert len(result.children) > 0
                assert result.children[0].name == ".sys"

            driver_config_for_root = ydb.DriverConfig(
                "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
                self.root_dir
            )

            with ydb.Driver(driver_config_for_root) as driver:
                driver.wait()

                result = driver.scheme_client.list_directory(database_path)
                logger.debug("From root: list database <%s> is %s", database_path, convert(result))
                assert len(result.children) > 0
                assert result.children[0].name == ".sys"

                result = driver.scheme_client.list_directory(above_database)
                logger.debug("From root: list above database <%s> is %s", above_database, convert(result))
                assert len(result.children) > 0
                assert result.children[0].name == basename
                assert result.children[0].type == ydb.scheme.SchemeEntryType.DATABASE


class TestYqlLocks(DBForStaticSlots):
    def _create_tables(self, pool):
        def callee(session):
            session.execute_scheme(
                "create table bills (account Uint64, deposit Int64, primary key(account)); "
                "create table transfers "
                "(tx Uint64, acc_from Uint64, acc_to Uint64, amount Int64, primary key(tx)); "
            )
        pool.retry_operation_sync(callee, self.robust_retries)

    @staticmethod
    def _initial_credit(pool):
        with pool.checkout() as session:
            session.transaction().execute(
                "upsert into bills (account, deposit) "
                "values (1u, 1000), (2u, 1000); "
                "upsert into transfers (tx, acc_from, acc_to, amount) "
                "values (0u, 0u, 1u, 1000), (1u, 0u, 2u, 1000); ",
                commit_tx=True,
            )

    @staticmethod
    def _plan_transactions(pool):
        operations = []
        for x in range(1, 10):
            operations += [x, -x]
        random.shuffle(operations)
        return zip(range(2, 2 + len(operations)), operations)

    @staticmethod
    def _perform_transaction(pool, tx, operation):
        with pool.checkout() as session:
            session.transaction().execute(
                "$delta = ({delta}); "
                "$tx = ({tx}); "
                "$first = (select account, deposit from bills where account = 1u); "
                "$second = (select account, deposit from bills where account = 2u); "
                ""
                "upsert into bills (account, deposit) "
                "select account as account, deposit - $delta as deposit FROM $first; "
                ""
                "upsert into bills (account, deposit) "
                "select account as account, deposit + $delta as deposit FROM $second; "
                ""
                "upsert into transfers (tx, acc_from, acc_to, amount) "
                "values ($tx, 1u, 2u, $delta); ".format(delta=operation, tx=tx),
                commit_tx=True,
            )

    @staticmethod
    def _control_read(pool):
        account_first = "select account, deposit from bills where account = 1u;"
        account_second = "select account, deposit from bills where account = 2u;"
        transfers = "select tx, acc_from, acc_to, amount from transfers; "

        data = []
        with pool.checkout() as session:
            with session.transaction() as tx:
                for query in (account_first, account_second, transfers):
                    data.append(
                        tx.execute(
                            query
                        )
                    )

                tx.commit()
        assert len(data) > 0
        return data

    def test_operation_with_locks(self):
        with Runtime(self.cluster, self.database_name):
            with ydb.Driver(self.driver_config) as driver:
                with ydb.SessionPool(driver, size=1) as pool:

                    self._create_tables(pool)

                    self._initial_credit(pool)

                    transactions = self._plan_transactions(pool)

                    for tx, operation in transactions:
                        logger.debug("transaction %s operation %s", tx, operation)
                        self._perform_transaction(
                            pool,
                            tx,
                            operation
                        )

                    logger.debug("read with two hops")
                    account_1, account_2, transfers = self._control_read(pool)

                    assert account_1[0].rows[0].account == 1 and account_1[0].rows[0].deposit == 1000
                    assert account_2[0].rows[0].account == 2 and account_1[0].rows[0].deposit == 1000

                    sum = 0
                    for tx, row in zip(range(len(transfers[0].rows)), transfers[0].rows):
                        assert tx == row.tx and row.amount != 0
                        sum += row.amount

                    assert sum == 2000
