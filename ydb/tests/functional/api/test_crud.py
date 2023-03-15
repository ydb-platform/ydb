# -*- coding: utf-8 -*-
import logging
import pytest
import random

from hamcrest import assert_that, equal_to, raises

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.oss.ydb_sdk_import import ydb


logger = logging.getLogger(__name__)


class TestCreateAndUpsertWithRepetitions(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint="%s:%s" % (
                    cls.cluster.nodes[1].host, cls.cluster.nodes[1].port
                )
            )
        )
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'driver'):
            cls.driver.stop()

        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    @pytest.mark.parametrize(['repetitions', 'column_count'], [(10, 64)])
    def test_create_and_select_with_repetitions(self, repetitions, column_count):
        for idx in range(repetitions):
            table_name = "test_create_and_select_with_repetitions_%d" % idx
            session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
            columns = ["column_%d" % idx for idx in range(column_count)]
            session.execute_scheme(
                "CREATE TABLE %s (%s, PRIMARY KEY(%s))" % (
                    table_name, ", ".join("%s Utf8" % column for column in columns), columns[0]
                )
            )

            with session.transaction() as tx:
                tx.execute('select * from %s;' % table_name)

    @pytest.mark.parametrize(['repetitions', 'column_count'], [(10, 64)])
    def test_create_and_upsert_data_with_repetitions(self, repetitions, column_count):
        for idx in range(repetitions):
            table_name = "test_create_and_upsert_data_with_repetitions_%d" % idx
            session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
            columns = ['column_%d' % idx for idx in range(column_count)]
            session.execute_scheme(
                "CREATE TABLE %s (%s, PRIMARY KEY(%s))" % (
                    table_name, ", ".join("%s Utf8" % column for column in columns), columns[0]
                )
            )

            value = ', '.join('"%s"' % str(idx) for idx in columns)
            session.transaction().execute(
                "UPSERT INTO %s (%s) VALUES (%s);" % (table_name, ', '.join(columns), value),
                commit_tx=True
            )


class TestCRUDOperations(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(KikimrConfigGenerator(load_udfs=True))
        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint="%s:%s" % (
                    cls.cluster.nodes[1].host, cls.cluster.nodes[1].port
                )
            )
        )
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'driver'):
            cls.driver.stop()

        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_create_table_and_drop_table_success(self):
        name = "test_create_table_and_drop_table_success"
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.execute_scheme("CREATE TABLE %s (col1 String, PRIMARY KEY(col1));" % name)
        session.execute_scheme("DROP TABLE %s;" % name)

    def test_create_table_wrong_primary_key_failed1(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        def callee():
            session.execute_scheme(
                'create table test_create_table_wrong_primary_key_failed1 (id Int, primary key(invalid));'
            )

        assert_that(
            callee,
            raises(
                ydb.GenericError,
                "Undefined column: invalid",
            )
        )

    def test_create_table_wrong_primary_key_failed2(self):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        name = "test_create_table_wrong_primary_key_failed2"

        def callee():
            session.execute_scheme("CREATE TABLE %s (col1 int)" % name)

        assert_that(
            callee,
            raises(
                ydb.GenericError,
                "Primary key is required for ydb tables",
            )
        )


class TestSelect(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(KikimrConfigGenerator(load_udfs=True))
        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint="%s:%s" % (
                    cls.cluster.nodes[1].host,
                    cls.cluster.nodes[1].port
                )
            )
        )

        session = ydb.retry_operation_sync(lambda: cls.driver.table_client.session().create())
        session.execute_scheme(
            "CREATE TABLE t1 (rowid int, a Utf8, b Utf8, PRIMARY KEY(rowid)); "
            "CREATE TABLE t2 (rowid int, a Utf8, b Utf8, PRIMARY KEY(rowid)); "
            "CREATE TABLE t3 (rowid int, a Utf8, c int, PRIMARY KEY(rowid)); "
            "CREATE TABLE t4 (rowid int, a Utf8, c int, PRIMARY KEY(rowid)); "
        )

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    @pytest.mark.parametrize(['cmd', 'message'], [
        ("select distinct b, a from (select a, b from t1 union all select b, a from t1 order by b) order by B",
         "Column B is not in source column set.*"),
        ("select count(a, b) from t1", "Aggregation function Count requires exactly 1 argument"),
        ("select min(a, b) from t1", "Aggregation function Min requires exactly 1 argument"),
        ("select min(*) from t1", ".*is not allowed here"),
    ])
    def test_advanced_select_failed(self, cmd, message):

        def callee():
            session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
            try:
                with session.transaction() as tx:
                    tx.execute(cmd, commit_tx=True)

            finally:

                session.delete()

        assert_that(
            callee,
            raises(
                ydb.GenericError,
                message,
            )
        )


class TestClientTimeouts(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()
        cls.driver_config = ydb.DriverConfig(
            database='/Root', endpoint="%s:%s" % (
                cls.cluster.nodes[1].host, cls.cluster.nodes[1].port))
        cls.driver = ydb.Driver(cls.driver_config)
        cls.driver.wait()
        cls.pool = ydb.SessionPool(cls.driver)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

        if hasattr(cls, 'driver'):
            cls.driver.stop()

    def _on_query_response(self, f, session):
        try:
            f.result()
        except Exception:
            pass

        finally:
            self.pool.release(session)

    def _send_query(self):
        rq = ydb.BaseRequestSettings().with_cancel_after(0.02).with_operation_timeout(0.04)
        session = self.pool.acquire()
        f = session.transaction().async_execute('select 1', commit_tx=True, settings=rq)
        f.add_done_callback(
            lambda _: self._on_query_response(
                _, session
            )
        )
        return f

    def test_can_set_timeouts_on_query(self):
        fs = [self._send_query() for _ in range(100)]

        for f in fs:
            try:
                f.result()
            except (ydb.Timeout, ydb.Cancelled) as e:
                logger.info("Received error on request %s", e.status.name)


class TestManySelectsInRow(object):
    """
    Test for bug KIKIMR-3109
    """
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint="%s:%s" % (
                    cls.cluster.nodes[1].host, cls.cluster.nodes[1].port
                )
            )
        )
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

        if hasattr(cls, 'driver'):
            cls.driver.stop()

    @staticmethod
    def __get_random_entry():
        idx = random.randint(2 ** 40, 2 ** 50)
        timestamp = random.randint(2 ** 40, 2 ** 50)
        message = "".join([str(d_v) for d_v in range(20)])
        rest = "".join([str(d_v) for d_v in range(20)])
        return idx, timestamp, message, rest

    @pytest.mark.parametrize(
        ['entry_count', 'query_count', 'limit_size'],
        [(500, 500, 50)]
    )
    def test_selects_in_row_success(self, entry_count, query_count, limit_size):
        name = "test_selects_in_row_success"
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.execute_scheme(
            "CREATE TABLE %s (`idx` Uint64, `timestamp` Uint64, `rest` Utf8, `message` Utf8, PRIMARY KEY(idx))" % name
        )

        with session.transaction() as tx:
            for _ in range(entry_count):
                tx.execute(
                    'UPSERT INTO test_selects_in_row_success '
                    '(`idx`, `timestamp`, `rest`, `message`) '
                    'VALUES (%d, %d, "%s", "%s")' % self.__get_random_entry()
                )
            tx.commit()

        for query_id in range(1, query_count + 1):
            with session.transaction() as tx:
                result_sets = tx.execute(
                    "select * from %s ORDER BY `timestamp` LIMIT %d" % (
                        name, limit_size
                    )
                )

                assert_that(
                    result_sets[0].truncated,
                    equal_to(False),
                    "Failed on query %d" % query_id
                )
