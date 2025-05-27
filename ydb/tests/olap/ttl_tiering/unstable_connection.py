import time
import logging
from .base import TllTieringTestBase
from ydb.tests.olap.common.column_table_helper import ColumnTableHelper
import ydb
import concurrent
import random
import datetime
import threading
import subprocess

from ydb.tests.library.test_meta import link_test_case

logger = logging.getLogger(__name__)


class TestUnstableConnection(TllTieringTestBase):
    test_name = "unstable_connection"
    cold_bucket = "cold"
    num_writers = 10
    num_readers = 4

    @classmethod
    def setup_class(cls):
        super(TestUnstableConnection, cls).setup_class()
        cls.s3_client.create_bucket(cls.cold_bucket)

    def write_data(
        self,
        table: str,
        timestamp_from_ms: int,
        rows: int,
    ):
        chunk_size = 100
        while rows:
            current_chunk_size = min(chunk_size, rows)
            data = [
                {
                    'ts': timestamp_from_ms + i,
                    's': random.randbytes(1024),
                } for i in range(current_chunk_size)
            ]
            self.ydb_client.bulk_upsert(
                table,
                self.column_types,
                data,
            )
            timestamp_from_ms += current_chunk_size
            rows -= current_chunk_size
            assert rows >= 0

    def writer(self, table: str, stop_event):
        logger.info("Writer started")
        while not stop_event.is_set():
            self.write_data(table, int(datetime.datetime.now().timestamp() * 1000000), 1000)
        logger.info("Writer stopped")

    def reader(self, table: str, stop_event):
        logger.info("Reader started")
        while not stop_event.is_set():
            self.ydb_client.query(f"SELECT ts FROM `{table}` WHERE StartsWith(s, \"a\")")
        logger.info("Reader stopped")

    def adversary(self, pid: int, stop_event):
        while not stop_event.is_set():
            logger.info(f"Stopping s3 process, pid={pid}")
            subprocess.run(["kill", "-STOP", str(pid)])
            time.sleep(1)
            logger.info(f"Resuming s3 process, pid={pid}")
            subprocess.run(["kill", "-CONT", str(pid)])
            time.sleep(2)
        subprocess.run(["kill", "-CONT", str(pid)])
        logger.info("Adversary stopped")

    def stopwatch(self, seconds: int, stop_event):
        time.sleep(seconds)
        stop_event.set()

    @link_test_case("#13544")
    def test(self):
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/table"
        secret_prefix = self.test_name
        access_key_id_secret_name = f"{secret_prefix}_key_id"
        access_key_secret_secret_name = f"{secret_prefix}_key_secret"
        eds_path = f"{test_dir}/{self.cold_bucket}"

        # Expect empty buckets to avoid unintentional data deletion/modification
        if self.s3_client.get_bucket_stat(self.cold_bucket) != (0, 0):
            raise Exception("Bucket for cold data is not empty")

        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                ts Timestamp NOT NULL,
                s String,
                val Uint64,
                PRIMARY KEY(ts),
            )
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4
            )
            """
        )

        table = ColumnTableHelper(self.ydb_client, table_path)
        table.set_fast_compaction()

        self.column_types = ydb.BulkUpsertColumns()
        self.column_types.add_column("ts", ydb.PrimitiveType.Timestamp)
        self.column_types.add_column("s", ydb.PrimitiveType.String)

        logger.info(f"Table {table_path} created")

        self.ydb_client.query(f"CREATE OBJECT {access_key_id_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_id}'")
        self.ydb_client.query(f"CREATE OBJECT {access_key_secret_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_secret}'")

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE `{eds_path}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{self.s3_client.endpoint}/{self.cold_bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            )
        """)

        stmt = f"""
            ALTER TABLE `{table_path}` SET (TTL =
                    Interval("PT1S") TO EXTERNAL DATA SOURCE `{eds_path}`,
                    Interval("PT1M") DELETE
                ON ts
            )
        """
        logger.info(stmt)
        self.ydb_client.query(stmt)

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_writers+self.num_readers+2) as executor:
            stop_event = threading.Event()
            workers = []

            for _ in range(self.num_writers):
                workers.append(executor.submit(self.writer, table_path, stop_event))

            for _ in range(self.num_readers):
                workers.append(executor.submit(self.reader, table_path, stop_event))

            workers.append(executor.submit(self.adversary, self.s3_pid, stop_event))
            workers.append(executor.submit(self.stopwatch, 120, stop_event))

            time.sleep(60)
            logger.info("Checking eviction")
            assert table.get_portion_stat_by_tier().get(eds_path, {}).get("Rows", 0), f"Nothing is evicted after 1 minute: {table.get_portion_stat_by_tier()}"

            concurrent.futures.wait(workers)
