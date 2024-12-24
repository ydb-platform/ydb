# -*- coding: utf-8 -*-
import ydb
import os
import random

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from concurrent.futures import ThreadPoolExecutor


class TestYdbInsertsOperations(object):

    @classmethod
    def setup_class(cls):
        cls.database = "/Root"
        cls.cluster = KiKiMR(KikimrConfigGenerator(erasure=Erasure.NONE))
        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database=cls.database,
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
        self.table_path = "insert_table_" + current_test_full_name.replace("::", ".").removesuffix(" (setup)")
        print(self.table_path)

    def test_insert_multiple_rows(self):
        """
        Test inserting multiple rows and verify all are present
        """
        table_name = f"{self.table_path}_multiple"

        self.pool.execute_with_retries(
            f"""
            CREATE TABLE `{table_name}` (
                id Int64 NOT NULL,
                value Utf8 NOT NULL,
                PRIMARY KEY (id)
            )
            PARTITION BY HASH(id)
            WITH(STORE=COLUMN)
            """
        )

        # Insert multiple rows
        for i in range(100):
            self.pool.execute_with_retries(
                f"""
                UPSERT INTO `{table_name}` (id, value) VALUES ({i}, 'value_{i}');
                """
            )

        # Verify all rows are inserted
        result = self.pool.execute_with_retries(
            f"""
            SELECT COUNT(*) as count FROM `{table_name}`;
            """
        )
        rows = result[0].rows
        assert len(rows) == 1 and rows[0].count == 100, f"Expected 100 rows, found: {rows[0].count}"

    def test_concurrent_inserts(self):
        """
        Test concurrent insertion and verify data integrity
        """
        table_name = f"{self.table_path}_concurrent"

        self.pool.execute_with_retries(
            f"""
            CREATE TABLE `{table_name}` (
                id Int64 NOT NULL,
                value Utf8 NOT NULL,
                PRIMARY KEY (id)
            )
            PARTITION BY HASH(id)
            WITH(STORE=COLUMN)
            """
        )

        # Function to insert data in parallel
        def insert_data(offset):
            for i in range(50):
                self.pool.execute_with_retries(
                    f"""
                    UPSERT INTO `{table_name}` (id, value) VALUES ({offset + i}, 'value_{offset + i}');
                    """
                )

        # Execute inserts in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(insert_data, offset) for offset in [0, 50]]

        # Confirm all tasks completed
        for future in futures:
            future.result()

        # Verify all rows are inserted
        result = self.pool.execute_with_retries(
            f"""
            SELECT COUNT(*) as count FROM `{table_name}`;
            """
        )
        rows = result[0].rows
        assert len(rows) == 1 and rows[0].count == 100, f"Expected 100 rows, found: {rows[0].count}"

    # def test_transactional_update(self):
    #     """
    #     Test updating data with transaction
    #     """
    #     table_name = f"{self.table_path}_transaction"

    #     self.pool.execute_with_retries(
    #         f"""
    #         CREATE TABLE `{table_name}` (
    #             id Int64 NOT NULL,
    #             value Utf8 NOT NULL,
    #             PRIMARY KEY (id)
    #         )
    #         PARTITION BY HASH(id)
    #         WITH(STORE=COLUMN)
    #         """
    #     )

    #     # Insert initial data
    #     self.pool.execute_with_retries(
    #         f"""
    #         UPSERT INTO `{table_name}` (id, value) VALUES (1, 'initial_value');
    #         """
    #     )

    #     # Transactional Update
    #     with self.driver.table_client.session().create() as session:
    #         with session.transaction():
    #             session.execute(
    #                 f"""
    #                 UPDATE `{table_name}` SET value = 'transactional_update' WHERE id = 1;
    #                 """
    #             )
    #             session.commit()

    #     with self.driver.table_client.session().create() as session:
    #         with session.transaction():
    #             session.execute(
    #                 f"""
    #                 UPDATE `{table_name}` SET value = 'transactional_update_2' WHERE id = 1;
    #                 """
    #             )
    #             session.rollback()

    #     # Verify the update
    #     result = self.pool.execute_with_retries(
    #         f"""
    #         SELECT value FROM `{table_name}` WHERE id = 1;
    #         """
    #     )
    #     rows = result[0].rows
    #     assert len(rows) == 1, "Expected one row after transaction update"
    #     assert rows[0].value == 'transactional_update', "Transaction update did not commit properly"

    def test_bulk_upsert(self):
        """
        Test bulk upsert functionality
        """
        table_name = f"{self.table_path}_bulk"

        # Create table
        self.pool.execute_with_retries(
            f"""
            CREATE TABLE `{table_name}` (
                id Int64 NOT NULL,
                value Utf8 NOT NULL,
                PRIMARY KEY (id)
            )
            PARTITION BY HASH(id)
            WITH(STORE=COLUMN)
            """
        )

        # Prepare data for bulk upsert
        data = [{'id': i, 'value': f'value_{i}'} for i in range(1000)]

        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("id", ydb.PrimitiveType.Int64)
        column_types.add_column("value", ydb.PrimitiveType.Utf8)


        # Bulk Upsert
        self.driver.table_client.bulk_upsert(
            f"{self.database}/{table_name}",
            data,
            column_types
        )

        # Verify bulk upserted data
        result = self.pool.execute_with_retries(
            f"""
            SELECT COUNT(*) as count FROM `{table_name}`;
            """
        )
        rows = result[0].rows
        assert len(rows) == 1 and rows[0].count == 1000, f"Expected 1000 rows, found: {rows[0].count}"

        # Check random rows
        sample_ids = random.sample(range(1000), 10)  # Select 10 random ids to verify
        for id_ in sample_ids:
            result = self.pool.execute_with_retries(
                f"""
                SELECT value FROM `{table_name}` WHERE id = {id_};
                """
            )
            rows = result[0].rows
            assert len(rows) == 1, f"Expected one row for id {id_}"
            assert rows[0].value == f'value_{id_}', f"Value mismatch for id {id_}, expected 'value_{id_}'"


    def test_bulk_upsert_same_values(self):
        """
        Test bulk upsert functionality
        """
        table_name = f"{self.table_path}_bulk"

        # Create table
        self.pool.execute_with_retries(
            f"""
            CREATE TABLE `{table_name}` (
                id Int64 NOT NULL,
                value Utf8 NOT NULL,
                PRIMARY KEY (id)
            )
            PARTITION BY HASH(id)
            WITH(STORE=COLUMN)
            """
        )

        # Prepare data with duplicate keys
        data = [
            {'id': 1, 'value': 'initial_value'},
            {'id': 1, 'value': 'duplicate_value_1'},
            {'id': 2, 'value': 'unique_value'},
            {'id': 1, 'value': 'duplicate_value_2'}  # The last entry with id=1 should prevail
        ]

        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("id", ydb.PrimitiveType.Int64)
        column_types.add_column("value", ydb.PrimitiveType.Utf8)


        # Bulk Upsert
        self.driver.table_client.bulk_upsert(
            f"{self.database}/{table_name}",
            data,
            column_types
        )

        # Verify that the last value persists for duplicate key
        result = self.pool.execute_with_retries(
            f"""
            SELECT id, value FROM `{table_name}`;
            """
        )
        rows = result[0].rows
        assert len(rows) == 2, f"Expected 2 unique rows, found: {len(rows)}"

        expected_values = {1: 'duplicate_value_2', 2: 'unique_value'}
        for row in rows:
            assert row.value == expected_values[row.id], f"Value mismatch for id {row.id}, expected {expected_values[row.id]}"

