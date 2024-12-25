# -*- coding: utf-8 -*-
import ydb
import pytest

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

    def test_transactional_update(self):
        """
        Test updating data with transaction
        """
        table_name = f"{self.table_path}_transaction"

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

        # Insert initial data
        self.query(
            f"""
            UPSERT INTO `{table_name}` (id, value) VALUES (1, 'initial_value');
            """
        )

        def process(session):
            tx = session.transaction().begin()
            self.query(
                f"""
                UPDATE `{table_name}` SET value = 'transactional_update' WHERE id = 1;
                """,
                tx
                )
            tx.commit()

            tx = session.transaction().begin()
            self.query(
                f"""
                UPDATE `{table_name}` SET value = 'transactional_update_2' WHERE id = 1;
                """,
                tx
                )
            tx.rollback()

        # Transactional Update
        self.transactional(process)

        # Verify the update
        return self.query(
            f"""
            SELECT value FROM `{table_name}` WHERE id = 1;
            """
        )

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

    def test_bulk_upsert_with_valid_and_invalid_data(self):
        """
        Test bulk upsert with a mix of valid and invalid data to ensure no data is inserted if any invalid data is present.
        """
        table_name = f"{self.table_path}_bulk_invalid"

        # Create table
        self.pool.execute_with_retries(
            f"""
            CREATE TABLE `{table_name}` (
                id Int64 NOT NULL,
                value Utf8 NOT NULL,
                PRIMARY KEY (id)
            );
            """
        )

        # Prepare data with one invalid entry
        valid_data = [
            {'id': 1, 'value': 'valid_value_1'},
            {'id': 2, 'value': 'valid_value_2'}
        ]
        invalid_data = [
            {'id': 'invalid_id', 'value': 'invalid_value'}  # invalid 'id' type
        ]

        # Combine data
        combined_data = valid_data + invalid_data

        # Attempt Bulk Upsert
        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("id", ydb.PrimitiveType.Int64)
        column_types.add_column("value", ydb.PrimitiveType.Utf8)

        with pytest.raises(TypeError):
            self.driver.table_client.bulk_upsert(
                f"{self.database}/{table_name}",
                combined_data,
                column_types
            )

        # Verify no data was inserted
        return self.query(
            f"""
            SELECT COUNT(*) as count FROM `{table_name}`;
            """
        )

    def test_bulk_upsert_parallel(self):
        """
        Test parallel insertion of 100,000 rows using bulk upsert and verify all data is inserted correctly.
        """
        table_name = f"{self.table_path}_bulk_parallel"

        # Create table
        self.pool.execute_with_retries(
            f"""
            CREATE TABLE `{table_name}` (
                id Int64 NOT NULL,
                value Utf8 NOT NULL,
                PRIMARY KEY (id)
            );
            """
        )

        # Function to perform bulk upsert
        def bulk_upsert_task(start, end):
            data = [{'id': i, 'value': f'value_{i}'} for i in range(start, end)]

            column_types = ydb.BulkUpsertColumns()
            column_types.add_column("id", ydb.PrimitiveType.Int64)
            column_types.add_column("value", ydb.PrimitiveType.Utf8)

            self.driver.table_client.bulk_upsert(
                f"{self.database}/{table_name}",
                data,
                column_types
            )

        # Number of rows and chunk size
        total_rows = 100000
        chunk_size = 10000  # Chunk size for each thread

        # Parallel execution
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(bulk_upsert_task, i, i + chunk_size)
                for i in range(0, total_rows, chunk_size)
            ]

            # Ensure all threads complete
            for future in futures:
                future.result()

        # Verify the total inserted rows
        rows = self.query(
            f"""
            SELECT id, value FROM `{table_name}` ORDER BY id ASC;
            """
        )

        assert len(rows) == total_rows, f"Expected {total_rows} rows, found: {len(rows)}"

        # Verify each row
        for i, row in enumerate(rows):
            expected_id = i
            expected_value = f"value_{i}"
            assert row.id == expected_id, f"Expected id {expected_id}, found: {row.id}"
            assert row.value == expected_value, f"Expected value {expected_value}, found: {row.value}"
