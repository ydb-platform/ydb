# -*- coding: utf-8 -*-
import os
import logging
import random
import time
import copy
import pytest

from hamcrest import assert_that, greater_than, is_, not_, none

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.harness.ydb_fixtures import ydb_database_ctx


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
)


@pytest.fixture(scope='module', params=[True, False], ids=['enable_alter_database_create_hive_first--true', 'enable_alter_database_create_hive_first--false'])
def enable_alter_database_create_hive_first(request):
    return request.param


# ydb_fixtures.ydb_cluster_configuration local override
@pytest.fixture(scope='module')
def ydb_cluster_configuration(enable_alter_database_create_hive_first):
    conf = copy.deepcopy(CLUSTER_CONFIG)
    conf['enable_alter_database_create_hive_first'] = enable_alter_database_create_hive_first
    return conf


class TestTenants():

    def test_create_remove_database(self, ydb_database):
        ydb_database

    def test_create_remove_database_wait(self, ydb_database):
        ydb_database
        time.sleep(5)

    def test_when_deactivate_fat_tenant_creation_another_tenant_is_ok(self, ydb_cluster, ydb_root, ydb_client_session, config_hive, robust_retries):
        logger.info("create fat tenant")

        boot_batch_size = 5
        config_hive(boot_batch_size=boot_batch_size)

        databases = [os.path.join(ydb_root, "database_{}".format(i)) for i in range(3)]

        for database_path in databases:
            with ydb_database_ctx(ydb_cluster, database_path, timeout_seconds=20):

                def create_table(session):
                    session.create_table(
                        os.path.join(database_path, 'table_0'),
                        ydb.TableDescription()
                        .with_column(ydb.Column('id', ydb.OptionalType(ydb.DataType.Uint64)))
                        .with_primary_key('id')
                        .with_profile(
                            ydb.TableProfile()
                            .with_partitioning_policy(
                                ydb.PartitioningPolicy().with_uniform_partitions(boot_batch_size * 2)
                            )
                        )
                    )

                def describe_table(session):
                    result = session.describe_table(os.path.join(database_path, 'table_0'))
                    logger.debug("> describe table: series, %s", str(result))
                    return result

                pool = ydb_client_session(database_path)

                pool.retry_operation_sync(create_table, robust_retries)
                pool.retry_operation_sync(describe_table)

    def test_register_tenant_and_force_drop_with_table(self, ydb_database, ydb_client_session, robust_retries):
        database_path = ydb_database
        pool = ydb_client_session(database_path)

        def create_table(session):
            session.create_table(
                os.path.join(database_path, 'table_0'),
                ydb.TableDescription()
                .with_column(ydb.Column('id', ydb.OptionalType(ydb.DataType.Uint64)))
                .with_primary_key('id')
            )

        pool.retry_operation_sync(create_table, robust_retries)

    def test_force_delete_tenant_when_table_has_been_deleted(self, ydb_database, ydb_client_session, robust_retries):
        database_path = ydb_database
        pool = ydb_client_session(database_path)

        def create_table(session):
            session.create_table(
                os.path.join(database_path, 'table_0'),
                ydb.TableDescription()
                .with_column(ydb.Column('id', ydb.OptionalType(ydb.DataType.Uint64)))
                .with_primary_key('id')
            )

        def describe_table(session):
            result = session.describe_table(os.path.join(database_path, 'table_0'))
            logger.debug("> describe table: series, %s", str(result))
            return result

        pool.retry_operation_sync(create_table, robust_retries)
        pool.retry_operation_sync(describe_table)
        with pool.checkout() as session:
            session.drop_table(os.path.join(database_path, 'table_0'))

    def test_progress_when_tenant_tablets_run_on_dynamic_nodes(self, ydb_database, ydb_client_session, robust_retries):
        database_path = ydb_database
        pool = ydb_client_session(database_path)

        def create_table(session):
            session.create_table(
                os.path.join(database_path, 'table_0'),
                ydb.TableDescription()
                .with_column(ydb.Column('id', ydb.OptionalType(ydb.DataType.Uint64)))
                .with_primary_key('id')
            )

        def describe_table(session):
            result = session.describe_table(os.path.join(database_path, 'table_0'))
            logger.debug("> describe table: series, %s", str(result))
            return result

        def drop_table(session):
            result = session.drop_table(os.path.join(database_path, 'table_0'))
            logger.debug("> drop table: series, %s", str(result))
            return result

        pool.retry_operation_sync(create_table, robust_retries)
        pool.retry_operation_sync(describe_table)
        pool.retry_operation_sync(drop_table)

    def test_yql_operations_over_dynamic_nodes(self, ydb_database, ydb_client_session, robust_retries):
        database_path = ydb_database
        pool = ydb_client_session(database_path)

        table_path_1 = os.path.join(database_path, 'table_1')
        table_path_2 = os.path.join(database_path, 'table_2')

        def create_table(session):
            session.execute_scheme(
                fr'''
                create table `{table_path_1}` (key Int32, value String, primary key(key));
                create table `{table_path_2}` (key Int32, value String, primary key(key));
                '''
            )

        def upsert(session):
            session.transaction().execute(
                fr'''
                upsert into `{table_path_1}` (key) values (101);
                upsert into `{table_path_2}` (key) values (102);
                ''',
                commit_tx=True
            )

        def select(session):
            session.transaction().execute(
                fr'''
                select key from `{table_path_1}`;
                select key from `{table_path_2}`;
                ''',
                commit_tx=True
            )

        pool.retry_operation_sync(create_table, robust_retries)
        pool.retry_operation_sync(upsert)
        pool.retry_operation_sync(select)

    def test_resolve_nodes(self, ydb_cluster, ydb_root, ydb_safe_test_name, ydb_client, robust_retries):
        database_path = os.path.join(ydb_root, ydb_safe_test_name)
        ydb_cluster.create_database(
            database_path,
            storage_pool_units_count={
                'hdd': 1
            },
            timeout_seconds=20
        )
        driver = ydb_client(database_path)

        try:
            driver.wait(robust_retries.get_session_client_timeout)
        except Exception as e:
            logger.info("failed to find endpoints as expected: %s", e)
            errors = driver.discovery_debug_details()
            logger.info("discovery details: %s", errors)
        else:
            logger.exception("endpoints have found, not expected")
            assert False

        database_nodes = ydb_cluster.register_and_start_slots(database_path, count=1)

        with ydb.SessionPool(driver, size=1) as pool:
            def callee(session):
                session.execute_scheme(
                    "CREATE TABLE warmUp (id utf8, PRIMARY KEY (id));"
                )
            pool.retry_operation_sync(callee, robust_retries)

        ydb_cluster.remove_database(
            database_path,
            timeout_seconds=20
        )

        ydb_cluster.unregister_and_stop_slots(database_nodes)

    def test_create_tables(self, ydb_database, ydb_client_session, robust_retries):
        database_path = ydb_database
        pool = ydb_client_session(database_path)

        def warmUp(session):
            session.execute_scheme(
                "CREATE TABLE warmUp (id utf8, PRIMARY KEY (id));"
            )

        pool.retry_operation_sync(warmUp, robust_retries)

        create_futures = []
        table = os.path.join(database_path, "temp/hardware/default/compute_az", "allocations")
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
                logger.info("ydb.Unavailable: %s", e)

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

    def test_stop_start(self, ydb_cluster, ydb_client_session, ydb_root, ydb_safe_test_name, robust_retries):
        def create_table(session, table):
            session.execute_scheme(fr'''
                CREATE TABLE `{table}` (id utf8, PRIMARY KEY (id));
            ''')

        database_path = os.path.join(ydb_root, ydb_safe_test_name)

        for i in range(5):
            with ydb_database_ctx(ydb_cluster, database_path):
                pool = ydb_client_session(database_path)
                pool.retry_operation_sync(create_table, robust_retries, 'table_{}'.format(i))

    def test_create_drop_create_table(self, ydb_database, ydb_client_session, robust_retries):
        database_path = ydb_database
        pool = ydb_client_session(database_path)

        table = os.path.join(database_path, 'table_0')

        def create_table(session):
            session.create_table(
                table,
                ydb.TableDescription()
                .with_column(ydb.Column('id', ydb.OptionalType(ydb.DataType.Uint64)))
                .with_primary_key('id')
            )

        def drop_table(session):
            session.drop_table(
                table
            )

        pool.retry_operation_sync(create_table, robust_retries)
        pool.retry_operation_sync(drop_table, robust_retries)
        pool.retry_operation_sync(create_table, robust_retries)

    def test_create_drop_create_table2(self, ydb_database, ydb_client_session, robust_retries):
        database_path = ydb_database
        pool = ydb_client_session(database_path)

        table = os.path.join(database_path, 'table_0')

        def create_drop_create_table(session):
            session.create_table(
                table,
                ydb.TableDescription()
                .with_column(ydb.Column('id', ydb.OptionalType(ydb.DataType.Uint64)))
                .with_primary_key('id')
            )
            session.drop_table(
                table,
            )
            session.create_table(
                table,
                ydb.TableDescription()
                .with_column(ydb.Column('id_1', ydb.OptionalType(ydb.DataType.Uint64)))
                .with_primary_key('id_1')
            )

        pool.retry_operation_sync(create_drop_create_table, robust_retries)

    @pytest.mark.xfail
    def test_create_drop_create_table3(self, ydb_database, ydb_client_session, robust_retries):
        database_path = ydb_database
        pool = ydb_client_session(database_path)

        table = os.path.join(database_path, 'table_0')

        def create_table(session):
            session.execute_scheme(fr'''
                CREATE TABLE `{table}`
                (
                    id Int64,
                    primary key (id)
                );
            ''')

        def drop_create_table(session):
            session.execute_scheme(fr'''
                DROP TABLE `{table}`;
                CREATE TABLE `{table}`
                (
                    id_1 Int64,
                    primary key (id_1)
                );
            ''')

        pool.retry_operation_sync(create_table, robust_retries)
        pool.retry_operation_sync(drop_create_table, robust_retries)

    def test_create_create_table(self, ydb_database, ydb_client_session, robust_retries):
        database_path = ydb_database
        pool = ydb_client_session(database_path)

        def create_tables(session, table_base):
            session.execute_scheme(fr'''
                CREATE TABLE `{table_base}_0`
                (
                    id Int64,
                    primary key (id)
                );
                CREATE TABLE `{table_base}_1`
                (
                    id Int64,
                    primary key (id)
                );
            ''')

        pool.retry_operation_sync(create_tables, robust_retries, "first_two")
        pool.retry_operation_sync(create_tables, robust_retries, "second_two")

    def test_list_database_above(self, ydb_database, ydb_endpoint, ydb_root):
        def convert(item):
            def _get_entry_schema(entry):
                return dict(
                    name=os.path.basename(entry.name),
                    type=str(entry.type),
                    owner=entry.owner,
                    effective_permissions=[x.to_pb() for x in entry.effective_permissions],
                    permissions=[x.to_pb() for x in entry.permissions],
                )

            if isinstance(item, ydb.scheme.Directory):
                d = dict(scheme_type='Directory')
                d.update(_get_entry_schema(item))
                d.update(dict(
                    children=[convert(x) for x in item.children]
                ))
            elif isinstance(item, ydb.scheme.SchemeEntry):
                d = dict(scheme_type='SchemeEntry')
                d.update(_get_entry_schema(item))
            else:
                d = dict(scheme_type='UnknownEntry:{}'.format(type(item)))
                d.update(_get_entry_schema(item))
            return d

        database_path = ydb_database

        driver_config = ydb.DriverConfig(ydb_endpoint, database_path)
        with ydb.Driver(driver_config) as driver:
            driver.wait()

            result = driver.scheme_client.list_directory(database_path)
            logger.debug("From database: list database <%s> is %s", database_path, convert(result))
            assert len(result.children) > 0
            assert result.children[0].name == ".sys"

        driver_config_for_root = ydb.DriverConfig(ydb_endpoint, ydb_root)
        with ydb.Driver(driver_config_for_root) as driver:
            driver.wait()

            result = driver.scheme_client.list_directory(database_path)
            logger.debug("From root: list database <%s> is %s", database_path, convert(result))
            assert len(result.children) > 0
            assert result.children[0].name == ".sys"

            dirname, basename = os.path.split(database_path)
            result = driver.scheme_client.list_directory(dirname)
            logger.debug("From root: list above database <%s> is %s", dirname, convert(result))
            assert len(result.children) > 0
            assert result.children[0].name == basename
            assert result.children[0].type == ydb.scheme.SchemeEntryType.DATABASE


def _initial_credit(pool):
    with pool.checkout() as session:
        session.transaction().execute(
            "upsert into bills (account, deposit) "
            "values (1u, 1000), (2u, 1000); "
            "upsert into transfers (tx, acc_from, acc_to, amount) "
            "values (0u, 0u, 1u, 1000), (1u, 0u, 2u, 1000); ",
            commit_tx=True,
        )


def _plan_transactions():
    operations = []
    for x in range(1, 10):
        operations += [x, -x]
    random.shuffle(operations)
    return zip(range(2, 2 + len(operations)), operations)


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


def test_operation_with_locks(ydb_database, ydb_client, robust_retries):
    def create_tables(session):
        session.execute_scheme(r'''
            create table bills (account Uint64, deposit Int64, primary key(account));
            create table transfers (tx Uint64, acc_from Uint64, acc_to Uint64, amount Int64, primary key(tx));
        ''')

    database_path = ydb_database
    driver = ydb_client(database_path)

    with ydb.SessionPool(driver, size=1) as pool:
        pool.retry_operation_sync(create_tables, robust_retries)

        _initial_credit(pool)

        transactions = _plan_transactions()

        for tx, operation in transactions:
            logger.debug("transaction %s operation %s", tx, operation)
            _perform_transaction(
                pool,
                tx,
                operation
            )

        logger.debug("read with two hops")
        account_1, account_2, transfers = _control_read(pool)

        assert account_1[0].rows[0].account == 1 and account_1[0].rows[0].deposit == 1000
        assert account_2[0].rows[0].account == 2 and account_1[0].rows[0].deposit == 1000

        sum = 0
        for tx, row in zip(range(len(transfers[0].rows)), transfers[0].rows):
            assert tx == row.tx and row.amount != 0
            sum += row.amount

        assert sum == 2000
