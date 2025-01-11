import concurrent.futures
import datetime
import random
import threading
import time
import boto3
import moto
import pytest
import requests
import ydb
import yatest.common
import logging
from ydb.tests.sql.lib.test_base import TestBase
from library.recipes import common as recipes_common


MOTO_SERVER_PATH = "contrib/python/moto/bin/moto_server"
S3_PID_FILE = "s3.pid"

class TestYdbS3TTL(TestBase):

    @pytest.fixture(scope="module")
    def s3(request) -> str:
        port_manager = yatest.common.network.PortManager()
        s3_port = port_manager.get_port()
        s3_url = "http://localhost:{port}".format(port=s3_port)

        command = [yatest.common.binary_path(MOTO_SERVER_PATH), "s3", "--host", "::1", "--port", str(s3_port)]

        def is_s3_ready():
            try:
                response = requests.get(s3_url)
                response.raise_for_status()
                return True
            except requests.RequestException as err:
                logging.debug(err)
                return False

        recipes_common.start_daemon(
            command=command, environment=None, is_alive_check=is_s3_ready, pid_file_name=S3_PID_FILE
        )

        try:
            yield s3_url
        finally:
            with open(S3_PID_FILE, 'r') as f:
                pid = int(f.read())
            recipes_common.stop_daemon(pid)



    def test_basic_tiering_operations(self):
        # Настройка мок S3
        mock_s3 = moto.mock_s3()
        mock_s3.start()

        # Инициализация S3 клиента и создание тестового бакета
        s3_client = boto3.client('s3')
        bucket_name = 'test-bucket'
        s3_client.create_bucket(Bucket=bucket_name)

        self.query(f"""
        CREATE EXTERNAL DATA SOURCE test_s3 WITH (
            SOURCE_TYPE="ObjectStorage",
            LOCATION="http://s3.amazonaws.com/{bucket_name}",
            AUTH_METHOD="NONE"
        );""")

        # Создание тестовой таблицы
        query = """
        CREATE TABLE test_table (
            id Uint64 not null,
            data Text,
            expire_at Timestamp not null,
            PRIMARY KEY (expire_at, id)
        ) PARTITION BY HASH(expire_at, id)
            WITH(STORE=COLUMN);

        ALTER TABLE test_table SET TTL
            Interval("PT10S") TO EXTERNAL DATA SOURCE `/Root/test_s3`
        ON expire_at;
        """
        self.query(query)

        num_threads = 10
        operations_per_thread = 10000

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            insert_futures = [executor.submit(self.writer, i, operations_per_thread) for i in range(num_threads)]

            # Wait for the insert operation to complete, then allow reading to complete
            concurrent.futures.wait(insert_futures)

        # Очистка
        self.mock_s3.stop()

    def writer(self, worker_id, num_operations):

        for i in range(num_operations):
            # Генерация тестовых данных
            record_id = random.randint(1, 1000000)
            data = f"test_data_{worker_id}_{i}"
            expire_at = datetime.datetime.now() + datetime.timedelta(hours=-random.randint(1, 48))

            # Запись в YDB
            query = """
            DECLARE $id AS Uint64;
            DECLARE $data AS Text;
            DECLARE $expire_at AS Timestamp;

            UPSERT INTO test_table (id, data, expire_at)
            VALUES ($id, $data, $expire_at);
            """
            parameters = {
                '$id': record_id,
                '$data': data,
                '$expire_at': expire_at
            }
            self.query(query, parameters=parameters)

            # Проверка записи в YDB
            query = """
            DECLARE $id AS Uint64;
            SELECT * FROM test_table WHERE id = $id;
            """
            self.query(
                query,
                parameters={'$id': record_id}
            )
