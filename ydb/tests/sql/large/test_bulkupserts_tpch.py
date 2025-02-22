# -*- coding: utf-8 -*-
import random
import ydb

from ydb.tests.library.test_meta import link_test_case
from ydb.tests.sql.lib.test_base import TpchTestBaseH1
from ydb.tests.sql.lib.helpers import split_data_into_fixed_size_chunks


class TestTpchBulkUpsertsOperations(TpchTestBaseH1):

    @link_test_case("#14643")
    def test_bulk_upsert_lineitem_with_overlapping_keys(self):
        """
        Test bulk upsert into the lineitem table with overlapping keys.
        Data are inserted by overlapping chunks
        """
        table_name = f"{self.tpch_default_path()}/lineitem"

        self.query(f"DELETE FROM `{table_name}` WHERE l_suppkey=9999")  # Special value for verification

        # Parameters for data generation
        total_records = 100_000
        num_chunks = 100
        records_per_chunk = total_records // num_chunks

        self.query(f"DELETE FROM `{table_name}` WHERE l_orderkey <= {2*total_records}")  # Remove possible duplicated keys

        # Store inserted data for verification
        inserted_data = []

        # Generate data for each worker with overlapping keys in lineitem
        def generate_lineitem_data(offset):
            data = [self.create_lineitem({
                'l_orderkey': i + offset,
                'l_linenumber': i + offset,
                'l_suppkey': 9999,  # Special value for verification
                'l_quantity': i + offset  # The "l_quantity" indicates the order, acting as a new "value"
                }) for i in range(records_per_chunk)
            ]
            inserted_data.extend(data)
            return data

        # records_per_chunk // 2 requred to make intervals overlapping
        data = [generate_lineitem_data(chunk_id * (records_per_chunk // 2)) for chunk_id in range(num_chunks)]

        # Execute bulk upserts in parallel
        for chunk in data:
            self.driver.table_client.bulk_upsert(
                f"{self.database}/{table_name}",
                chunk,
                self.tpch_bulk_upsert_col_types()
            )

        # Verify data integrity by fetching all records with the specific l_suppkey
        result_rows = self.query(
            f"""
            SELECT l_orderkey, l_quantity
            FROM `{table_name}`
            WHERE l_suppkey = 9999
            """
        )

        # Create a dictionary for easy data comparison
        inserted_dict = {row['l_orderkey']: row['l_quantity'] for row in inserted_data}
        result_dict = {row.l_orderkey: row['l_quantity'] for row in result_rows}

        # Compare inserted data with fetched data
        assert len(inserted_dict) == len(result_dict), "Mismatch in number of rows inserted and fetched"

        for key in inserted_dict:
            inserted_quantity = inserted_dict[key]
            result_quantity = result_dict.get(key)

            assert inserted_quantity == result_quantity, f"Mismatch in l_quantity for {key}"

    @link_test_case("#14642")
    def test_repeated_bulk_upsert_lineitem(self):
        """
        Test that repeatedly upserting records in the lineitem table with the same keys results,
        checking that only the latest record with the same key being stored.
        """
        table_name = f"{self.tpch_default_path()}/lineitem"

        # Cleanup overlapping data
        max_lineitem = self.query(f"SELECT max(l_orderkey) FROM `{table_name}`")[0][0] or self.tpch_est_records_count()
        self.query(f"DELETE FROM `{table_name}` WHERE l_suppkey=9999")  # Special value for verification

        # Create table to store generated ids. Thats ids must be deleted before test starts
        ids_table = f"{table_name}_ids"
        self.query(
            f"""
            CREATE TABLE `{ids_table}` (
                id Int64 NOT NULL,
                PRIMARY KEY (id)
            )
            PARTITION BY HASH(id)
            WITH(STORE=COLUMN)
            """
        )

        # Parameters for data generation
        num_keys = 1000
        upserts_per_key = 100

        # Generate data for repeated upserts with the same keys
        def generate_upsert_data():
            def generate_unique_key(key):
                while True:
                    key_index = key + random.randint(1, max_lineitem)
                    if key_index not in generated_keys:
                        return key_index

            data = []
            generated_keys = set()
            for key in range(num_keys):
                key_data = []

                key_index = generate_unique_key(key)

                generated_keys.add(key_index)
                for version in range(upserts_per_key):
                    key_data.append(self.create_lineitem({
                        'l_orderkey': key_index,
                        'l_linenumber': key_index,
                        'l_suppkey': 9999,  # Special value to filter for comparison
                        'l_quantity': version  # The "version" indicates the order, acting as a new "value"
                    }))
                random.shuffle(key_data)  # Shuffle to mix updates
                key_data.append(self.create_lineitem({
                    'l_orderkey': key_index,
                    'l_linenumber': key_index,
                    'l_suppkey': 9999,  # Special value to filter for comparison
                    'l_quantity': upserts_per_key  # The "version" indicates the order, acting as a new "value"
                }))
                data.extend(key_data)
            return data, list(generated_keys)

        upsert_data, generated_keys = generate_upsert_data()

        id_column_types = ydb.BulkUpsertColumns()
        id_column_types.add_column("id", ydb.PrimitiveType.Int64)
        self.driver.table_client.bulk_upsert(
            f"{self.database}/{ids_table}",
            [{"id": key} for key in generated_keys],
            id_column_types
        )

        # Remove ocassionally duplicated keys from existing database
        self.query(F"$ids = SELECT id from `{ids_table}`; DELETE FROM `{table_name}` WHERE l_orderkey in $ids")

        # Function to perform bulk upserts
        def bulk_upsert_operation(data_slice):
            self.driver.table_client.bulk_upsert(
                f"{self.database}/{table_name}",
                data_slice,
                self.tpch_bulk_upsert_col_types()
            )

        for chunk in split_data_into_fixed_size_chunks(upsert_data, 10000):
            bulk_upsert_operation(chunk)

        # Verify data integrity by fetching all records with the special l_suppkey
        result_rows = self.query(
            f"""
            SELECT l_orderkey, max(l_quantity) AS l_quantity
            FROM `{table_name}`
            WHERE l_suppkey = 9999
            GROUP BY l_orderkey;
            """
        )

        # Check each record to ensure only the last version is stored
        for row in result_rows:
            key = row.l_orderkey
            expected_quantity = upserts_per_key  # The last version should have this quantity
            assert row.l_quantity == expected_quantity, \
                f"Data mismatch for id {key}: expected quantity {expected_quantity}, got {row.l_quantity}"
