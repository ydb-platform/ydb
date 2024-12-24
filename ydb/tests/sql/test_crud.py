# -*- coding: utf-8 -*-
import ydb
import os

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure


class TestYdbCrudOperations(object):

    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(erasure=Erasure.NONE))
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
        cls.pool = ydb.QuerySessionPool(cls.driver)

    @classmethod
    def teardown_class(cls):
        cls.pool.stop()
        cls.driver.stop()
        cls.cluster.stop()

    def setup_method(self):
        current_test_full_name = os.environ.get("PYTEST_CURRENT_TEST")
        self.table_path = "crud_table_" + current_test_full_name.replace("::", ".").removesuffix(" (setup)")
        print(self.table_path)

    def test_crud_operations(self):
        """
        Test basic CRUD operations: Create, Read, Update, Delete
        """

        table_name = f"{self.table_path}"

        table_definition = f"""
            CREATE TABLE `{table_name}` (
                id Int64 NOT NULL,
                value Utf8 NOT NULL,
                PRIMARY KEY (id)
            )
            PARTITION BY HASH(id)
            WITH(STORE=COLUMN)
            """

        self.pool.execute_with_retries(
            table_definition
        )

        # Create (Insert)
        self.pool.execute_with_retries(
            f"""
            UPSERT INTO `{table_name}` (id, value) VALUES (1, 'initial_value');
            """
        )

        # Read
        result = self.pool.execute_with_retries(
            f"""
            SELECT value FROM `{table_name}` WHERE id = 1;
            """
        )
        rows = result[0].rows
        assert len(rows) == 1, "Expected one row after insert"
        assert rows[0].value == 'initial_value', "Value mismatch for inserted data"

        # Update
        self.pool.execute_with_retries(
            f"""
            UPDATE `{table_name}` SET value = 'updated_value' WHERE id = 1;
            """
        )
        result = self.pool.execute_with_retries(
            f"""
            SELECT value FROM `{table_name}` WHERE id = 1;
            """
        )
        rows = result[0].rows
        assert len(rows) == 1, "Expected one row after update"
        assert rows[0].value == 'updated_value', "Value mismatch after update"

        # Delete
        self.pool.execute_with_retries(
            f"""
            DELETE FROM `{table_name}` WHERE id = 1;
            """
        )
        result = self.pool.execute_with_retries(
            f"""
            SELECT value FROM `{table_name}` WHERE id = 1;
            """
        )
        rows = result[0].rows
        assert len(rows) == 0, "Expected no rows after delete"
