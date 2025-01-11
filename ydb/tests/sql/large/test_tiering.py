import concurrent.futures
import datetime
import random
import ydb
from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.sql.lib.test_s3 import S3Base
import time


class TestYdbS3TTL(TestBase, S3Base):

    def test_basic_tiering_operations(self):
        # Инициализация S3 клиента и создание тестового бакета

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
            PRIMARY KEY (expire_at, id)
        ) PARTITION BY HASH(expire_at, id)
            WITH(STORE=COLUMN);
        """)

        self.query(f"""
        ALTER TABLE `{self.table_path}_test_table` SET TTL
            Interval("PT10S") TO EXTERNAL DATA SOURCE `{self.database}/{self.table_path}_tests3`
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
        max_wait_time = 100  # Wait time for tiered files
        files = []
        while True:
            # Получаем текущий список объектов из бакета
            response = s3_client.list_objects_v2(Bucket=bucket_name)
            print(f"response={response}")

            # Проверяем, есть ли объекты в ответе
            if 'Contents' in response:
                files = [content['Key'] for content in response['Contents']]
                if len(files) > 0:
                    break

            # Проверяем, истекло ли максимальное время ожидания
            elapsed_time = time.time() - start_time
            if elapsed_time >= max_wait_time:
                break

            # Задержка перед следующей попыткой
            time.sleep(1)
        assert len(files) > 0, "No tiered files in bucket"

    def writer(self, worker_id, num_operations):
        upsert_data = []
        for i in range(num_operations):
            # Генерация тестовых данных
            record_id = random.randint(1, 1000000)
            data = f"test_data_{worker_id}_{i}"
            expire_at = datetime.datetime.now() + datetime.timedelta(hours=-random.randint(1, 48))

            upsert_data.append({"id": record_id, "data": data, "expire_at": expire_at})

        # Function to perform bulk upserts
        def bulk_upsert_operation(data_slice):
            column_types = ydb.BulkUpsertColumns()
            column_types.add_column("id", ydb.PrimitiveType.Uint64)
            column_types.add_column("data", ydb.PrimitiveType.Utf8)
            column_types.add_column("expire_at", ydb.PrimitiveType.Timestamp)

            self.driver.table_client.bulk_upsert(
                f"{self.database}/{self.table_path}_test_table",
                data_slice,
                column_types
            )

        for chunk in TestBase.split_data_into_fixed_size_chunks(upsert_data, 10000):
            bulk_upsert_operation(chunk)
