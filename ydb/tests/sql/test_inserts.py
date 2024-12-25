# -*- coding: utf-8 -*-
import ydb
import random

from concurrent.futures import ThreadPoolExecutor
from .test_base import TestBase


class TestYdbInsertsOperations(TestBase):

    def test_insert_multiple_rows(self):
        """
        Test inserting multiple rows and verify all are present
        """
        table_name = f"{self.table_path}_multiple"

        self.query(
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
            self.query(
                f"""
                UPSERT INTO `{table_name}` (id, value) VALUES ({i}, 'value_{i}');
                """
            )

        # Verify all rows are inserted
        rows = self.query(
            f"""
            SELECT COUNT(*) as count FROM `{table_name}`;
            """
        )
        assert len(rows) == 1 and rows[0].count == 100, f"Expected 100 rows, found: {rows[0].count}"

    def test_concurrent_inserts(self):
        """
        Test concurrent insertion and verify data integrity
        """
        table_name = f"{self.table_path}_concurrent"

        self.query(
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
                self.query(
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
        rows = self.query(
            f"""
            SELECT COUNT(*) as count FROM `{table_name}`;
            """
        )

        return rows

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
        self.query(
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

        return self.query(
                f"""
                SELECT * FROM `{table_name}` ORDER BY id asc;
                """
            )

    def test_bulk_upsert_same_values(self):
        """
        Test bulk upsert functionality
        """
        table_name = f"{self.table_path}_bulk"

        # Create table
        self.query(
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
        return self.query(
            f"""
            SELECT id, value FROM `{table_name}` ORDER BY id ASC;
            """
        )

    def test_bulk_upsert_same_values_simple(self):
        """
        Test bulk upsert functionality
        """
        table_name = f"{self.table_path}_bulk_simple"

        # Create table
        self.query(
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

        # Prepare data
        data = [
            {'id': 1, 'value': 'initial_value'},
            {'id': 2, 'value': 'unique_value'}
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
        return self.query(
            f"""
            SELECT id, value FROM `{table_name}` ORDER BY id ASC;
            """
        )
