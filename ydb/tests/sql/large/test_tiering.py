import concurrent.futures
import datetime
import random
import ydb
from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.sql.lib.helpers import split_data_into_fixed_size_chunks
from ydb.tests.sql.lib.test_s3 import S3Base
import time


class TestYdbS3TTL(TestBase, S3Base):

    def test_basic_tiering_operations(self):
        """
        Tests basic data tiering functionality with S3 storage.
        Sets up S3 bucket and external datasource, verifies data reading from external source.
        Creates table with TTL policy to move expired data to S3.
        Validates data migration to S3 tier after concurrent inserts and TTL expiration.
        """
        # S3 client initialization and temp bucket creation

        s3_client = self.s3_session_client()

        bucket_name = self.bucket_name()
        s3_client.create_bucket(Bucket=self.bucket_name())

        self.create_external_datasource_and_secrets(bucket_name)

        # Check if external datasource can read that data
        data = "a,b\n1,2"
        s3_client.put_object(Bucket=self.bucket_name(), Key="subfolder/test.csv", Body=data)

        results = self.query(f"""SELECT a, b FROM `{self.table_path}_external_datasource`.`subfolder/test.csv`
                                    WITH(FORMAT = 'csv_with_names',
                                         SCHEMA =
                                            (
                                                a Uint32,
                                                b Uint32
                                            )
                                    )""")

        assert results[0][0] == 1, "Check if read data from external data source"
        assert results[0][1] == 2, "Check if read data from external data source"

        # Create test table
        self.query(f"""
        CREATE TABLE `{self.table_path}_test_table` (
            id Uint64 not null,
            data Text,
            expire_at Timestamp not null,
            value Int64,
            PRIMARY KEY (expire_at, id)
        ) PARTITION BY HASH(expire_at, id)
          WITH(STORE=COLUMN);
        """)

        self.query(f"""
        ALTER TABLE `{self.table_path}_test_table` SET TTL
            Interval("PT10S") TO EXTERNAL DATA SOURCE `{self.database}/{self.table_path}_external_datasource`
        ON expire_at;
        """)

        num_threads = 10
        operations_per_thread = 100

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            insert_futures = [executor.submit(self.writer, i, operations_per_thread) for i in range(num_threads)]

            # Wait for the insert operation to complete, then allow reading to complete
            concurrent.futures.wait(insert_futures)
            [future.result() for future in insert_futures]

        start_time = time.time()
        max_wait_time = 150  # Wait time for tiered files
        while True:
            # Получаем текущий список объектов из бакета
            response = s3_client.list_objects_v2(Bucket=bucket_name)
            print(f"response={response}")

            # Проверяем, есть ли объекты в ответе
            if 'Contents' in response:
                files = [content['Key'] for content in response['Contents'] if not content['Key'].endswith(".csv")]
                if len(files) > 0:
                    break

            # Check if we need to stop by timeout
            elapsed_time = time.time() - start_time
            if elapsed_time >= max_wait_time:
                break

            # Wait until retry
            time.sleep(10)
        assert len(files) > 0, "No tiered files in bucket"

    def test_tiering_the_same_results(self):
        """
        Tests data consistency during tiering process to external storage.
        Verifies that:
        - Data remains consistent while being moved to S3
        - Multiple threads can safely insert data
        - Sum of values stays constant during tiering
        Test flow:
        1. Creates table and S3 bucket
        2. Parallel bulk inserts of test data
        3. Enables TTL with tiering
        4. Monitors data consistency during tiering process
        """

        s3_client = self.s3_session_client()

        bucket_name = self.bucket_name()
        s3_client.create_bucket(Bucket=self.bucket_name())

        self.create_external_datasource_and_secrets(bucket_name)

        # Create test table
        self.query(f"""
        CREATE TABLE `{self.table_path}_test_table` (
            id Uint64 not null,
            data Text,
            expire_at Timestamp not null,
            value Int64,
            PRIMARY KEY (expire_at, id)
        ) PARTITION BY HASH(expire_at, id)
            WITH(STORE=COLUMN);
        """)

        num_threads = 10
        operations_per_thread = 100

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            insert_futures = [executor.submit(self.writer, i, operations_per_thread) for i in range(num_threads)]

            # Wait for the insert operation to complete, then allow reading to complete
            concurrent.futures.wait(insert_futures)
            for future in insert_futures:
                future.result()

        total_value = self.query(f"SELECT sum(value) from `{self.table_path}_test_table`")[0][0]

        self.query(f"""
        ALTER TABLE `{self.table_path}_test_table` SET TTL
            Interval("PT10S") TO EXTERNAL DATA SOURCE `{self.database}/{self.table_path}_external_datasource`
        ON expire_at;
        """)

        start_time = time.time()
        max_wait_time = 150  # Wait time for tiered files

        while True:
            current_value = self.query(f"SELECT sum(value) from `{self.table_path}_test_table`")[0][0]
            assert current_value == total_value, f"Current value {current_value} is not equal to expected value {total_value}"

            # Check if we need to stop by timeout
            elapsed_time = time.time() - start_time
            if elapsed_time >= max_wait_time:
                break

            # Wait until retry
            time.sleep(1)

    def writer(self, worker_id, num_operations):
        upsert_data = []
        for i in range(num_operations):
            # Генерация тестовых данных
            record_id = random.randint(1, 1000000)
            data = f"test_data_{worker_id}_{i}"
            expire_at = datetime.datetime.now() - datetime.timedelta(hours=random.randint(1, 48))
            value = random.randint(1, 1000000)

            upsert_data.append({"id": record_id, "data": data, "expire_at": expire_at, "value": value})

        # Function to perform bulk upserts
        def bulk_upsert_operation(data_slice):
            column_types = ydb.BulkUpsertColumns()
            column_types.add_column("id", ydb.PrimitiveType.Uint64)
            column_types.add_column("value", ydb.PrimitiveType.Int64)
            column_types.add_column("data", ydb.PrimitiveType.Utf8)
            column_types.add_column("expire_at", ydb.PrimitiveType.Timestamp)

            self.driver.table_client.bulk_upsert(
                f"{self.database}/{self.table_path}_test_table",
                data_slice,
                column_types
            )

        for chunk in split_data_into_fixed_size_chunks(upsert_data, 10000):
            bulk_upsert_operation(chunk)
