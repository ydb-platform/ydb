# -*- coding: utf-8 -*-
import logging
import pytest
from hamcrest import assert_that, raises

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


class TestExecuteSchemeOperations(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR()
        cls.cluster.start()
        cls.driver_config = ydb.DriverConfig(
            "%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port), database='/Root')
        cls.driver = ydb.Driver(cls.driver_config)
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

        if hasattr(cls, 'driver'):
            cls.driver.stop()

    def test_create_table_if_it_is_created_success(self):
        name = 'test_create_table_if_it_is_created_success'
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        for idx in range(30):
            if idx % 3 == 2:
                session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
            session.execute_scheme(
                'CREATE TABLE %s (id Uint64, name String, PRIMARY KEY(id))' % name)

    @pytest.mark.parametrize("scheme_query, query", [
        (True, "CREATE TABLE PgUnsupported (id PgInt4, PRIMARY KEY (id))"),
        (False, "SELECT 1p"),
        (False, "DECLARE $value AS PgInt4; SELECT $value"),
        (False, "SELECT Pg::version()"),
        (False, "SELECT Pg::abs(1)"),
        (False, "SELECT ToPg(1)"),
        (False, "SELECT EvaluateExpr(FromPg(Pg::abs(1)))"),
        (False, "SELECT Nothing(ParseType('pgint4'))"),
    ])
    def test_pg_types_are_not_supported(self, scheme_query, query):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        try:
            with pytest.raises((ydb.BadRequest, ydb.GenericError), match="PostgreSQL types are not supported"):
                if scheme_query:
                    session.execute_scheme(query)
                else:
                    session.prepare(query)
            result = session.transaction().execute("SELECT 1 AS value", commit_tx=True)
            assert result[0].rows[0].value == 1
        finally:
            session.delete()

    def test_create_table_if_it_is_created_fail_add_new_column(self):
        # Arrange
        name = "test_create_table_if_it_is_created_fail_add_new_column"
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.execute_scheme(
            "CREATE TABLE %s (id1 Uint64, id2 Uint64, name String, PRIMARY KEY(id1, id2))" % name)

        # adding new column
        def callee():
            session.execute_scheme(
                "CREATE TABLE %s (id1 Uint64, id2 Uint64, name String, newcol String, PRIMARY KEY(id1, id2))" % name)

        assert_that(
            callee,
            raises(
                ydb.GenericError,
                "Table name conflict: .* is used to reference multiple tables",
            )
        )

    def test_create_table_if_it_is_created_fail_change_column_type(self):
        # Arrange
        name = "test_create_table_if_it_is_created_fail_change_column_type"
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.execute_scheme(
            "CREATE TABLE %s (id1 Uint64, id2 Uint64, name String, PRIMARY KEY(id1, id2))" % name)

        def callee():
            session.execute_scheme(
                "CREATE TABLE %s (id1 Uint64, id2 Uint64, name Uint64, PRIMARY KEY(id1, id2))" % name)

        assert_that(
            callee,
            raises(
                ydb.GenericError,
                "Table name conflict: .* is used to reference multiple tables",
            )
        )

    def test_create_table_if_it_is_created_fail_remove_column(self):
        # Arrange
        name = "test_create_table_if_it_is_created_fail_remove_column"
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.execute_scheme(
            "CREATE TABLE %s (id1 Uint64, id2 Uint64, name String, PRIMARY KEY(id1, id2))" % name)

        # removing column
        def callee():
            session.execute_scheme(
                "CREATE TABLE %s (id1 Uint64, id2 Uint64, PRIMARY KEY(id1, id2))" % name)

        assert_that(
            callee,
            raises(
                ydb.GenericError,
                "Table name conflict: .* is used to reference multiple tables",
            )
        )

    def test_create_table_if_it_is_created_fail_add_to_key(self):
        # Arrange
        name = "test_create_table_if_it_is_created_fail_add_to_key"
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.execute_scheme(
            "CREATE TABLE %s (id1 Uint64, id2 Uint64, name String, PRIMARY KEY(id1, id2))" % name)

        # adding something to key
        def callee():
            session.execute_scheme(
                "CREATE TABLE %s (id1 Uint64, id2 Uint64, name String, PRIMARY KEY(id1, id2, name))" % name)

        assert_that(
            callee,
            raises(
                ydb.GenericError,
                "Table name conflict: .* is used to reference multiple tables",
            )
        )

    def test_create_table_if_it_is_created_fail_remove_from_key(self):
        # Arrange
        name = "test_create_table_if_it_is_created_fail_remove_from_key"
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        session.execute_scheme(
            "CREATE TABLE %s (id1 Uint64, id2 Uint64, name String, PRIMARY KEY(id1, id2))" % name)

        # remove from key
        def callee():
            session.execute_scheme(
                "CREATE TABLE %s (id1 Uint64, id2 Uint64, name String, PRIMARY KEY(id1))" % name)

        assert_that(
            callee,
            raises(
                ydb.GenericError,
                "Table name conflict: .* is used to reference multiple tables",
            )
        )
