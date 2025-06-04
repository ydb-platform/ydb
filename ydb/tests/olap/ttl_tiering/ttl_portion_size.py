import logging
from .base import TllTieringTestBase
from ydb.tests.olap.common.column_table_helper import ColumnTableHelper
import concurrent
import random
import datetime
import ydb

logger = logging.getLogger(__name__)


class TestPortionSizeTtl(TllTieringTestBase):

    @classmethod
    def setup_class(cls):
        super(TestPortionSizeTtl, cls).setup_class()

    def get_row_count_by_date(self, table_path: str, past_days: int) -> int:
        return self.ydb_client.query(
            f"SELECT count(*) as Rows from `{table_path}` WHERE ts < CurrentUtcTimestamp() - DateTime::IntervalFromDays({past_days})"
        )[0].rows[0]["Rows"]

    def create_table(self, table_path):
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
                PARTITION_COUNT = 1
            )
            """
        )

    def drop_table(self, table_path):
        self.ydb_client.query(
            f"""
            DROP TABLE `{table_path}`
            """
        )

    def bulk_upsert(self, table_path):
        self.column_types = ydb.BulkUpsertColumns()
        self.column_types.add_column("ts", ydb.PrimitiveType.Timestamp)
        self.column_types.add_column("s", ydb.PrimitiveType.String)
        self.column_types.add_column("val", ydb.PrimitiveType.Uint64)

        ts_start = int(datetime.datetime.now().timestamp() * 1000000)
        rows = 10000
        num_threads = 10
        assert rows % num_threads == 0
        chunk_size = rows // num_threads
        # Write data
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            insert_futures = [
                executor.submit(
                    self.write_data,
                    table_path,
                    ts_start + i * chunk_size,
                    chunk_size,
                    1,
                )
                for i in range(num_threads)
            ]

            concurrent.futures.wait(insert_futures)
            for future in insert_futures:
                future.result()

        # Update data
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            insert_futures = [
                executor.submit(
                    self.write_data,
                    table_path,
                    ts_start + i * chunk_size,
                    chunk_size,
                    2,
                )
                for i in range(num_threads)
            ]

            concurrent.futures.wait(insert_futures)
            for future in insert_futures:
                future.result()

    def set_compaction(self, table_path, total_level_size):
        self.ydb_client.query(
            f"""
            ALTER OBJECT `{table_path}` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                  {{"levels" : [{{"class_name" : "Zero", "expected_blobs_size" : {total_level_size}}},
                                {{"class_name" : "Zero"}}]}}`);
            """
        )

    def write_data(
        self,
        table: str,
        timestamp_from_ms: int,
        rows: int,
        value: int = 1,
    ):
        chunk_size = 100
        while rows:
            current_chunk_size = min(chunk_size, rows)
            data = [
                {
                    "ts": timestamp_from_ms + i,
                    "s": random.randbytes(1000),
                    "val": value,
                }
                for i in range(current_chunk_size)
            ]
            self.ydb_client.bulk_upsert(
                table,
                self.column_types,
                data,
            )
            timestamp_from_ms += current_chunk_size
            rows -= current_chunk_size
            assert rows >= 0

    def set_tiering(self, table_path, bucket):
        secret_prefix = self.test_name
        access_key_id_secret_name = f"{secret_prefix}_key_id"
        access_key_secret_secret_name = f"{secret_prefix}_key_secret"
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        cold_eds_path = f"{test_dir}/{bucket}"
        self.ydb_client.query(
            f"UPSERT OBJECT {access_key_id_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_id}'"
        )
        self.ydb_client.query(
            f"UPSERT OBJECT {access_key_secret_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_secret}'"
        )

        self.ydb_client.query(
            f"""
            CREATE EXTERNAL DATA SOURCE `{cold_eds_path}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{self.s3_client.endpoint}/{bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            )
        """
        )

        stmt = f"""
            ALTER TABLE `{table_path}` SET (TTL =
                Interval("PT1S") TO EXTERNAL DATA SOURCE `{cold_eds_path}`
                ON ts
            )
        """

        self.ydb_client.query(stmt)

    def wait_tier_data(self, table_path, bucket):
        self.table = ColumnTableHelper(self.ydb_client, table_path)

        def data_distributes_across_tiers():
            bucket_stat = self.s3_client.get_bucket_stat(bucket)
            logger.info(
                f"portions: {self.table.get_portion_stat_by_tier()}, blobs: {self.table.get_blob_stat_by_tier()}, cold bucket stat: {bucket_stat}"
            )
            return bucket_stat[0] != 0

        assert self.wait_for(lambda: data_distributes_across_tiers(), 200), "Data eviction has not been started"

    def validate_portion_tier_size(self, table_path, left, right):
        results = self.ydb_client.query(
            f"select * from `{table_path}/.sys/primary_index_portion_stats` WHERE TierName != '__DEFAULT'"
        )
        for result_set in results:
            for row in result_set.rows:
                assert left <= row["ColumnBlobBytes"] and row["ColumnBlobBytes"] <= right, row

    def test_check_portion_size(self):
        self.test_name = 'check_portion_size_s3_tiering'
        cold_bucket_1 = 'cold1'
        cold_bucket_2 = 'cold2'
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path1 = f"{test_dir}/table1"
        table_path2 = f"{test_dir}/table2"

        self.s3_client.create_bucket(cold_bucket_1)

        # Expect empty buckets to avoid unintentional data deletion/modification
        if self.s3_client.get_bucket_stat(cold_bucket_1) != (0, 0):
            raise Exception("Bucket for cold data is not empty")

        self.create_table(table_path1)
        self.set_compaction(table_path1, 3000000)

        self.set_tiering(table_path1, cold_bucket_1)

        self.bulk_upsert(table_path1)
        self.wait_tier_data(table_path1, cold_bucket_1)
        self.validate_portion_tier_size(table_path1, 0.5 * 3000000, 2 * 3000000)

        self.s3_client.create_bucket(cold_bucket_2)

        # Expect empty buckets to avoid unintentional data deletion/modification
        if self.s3_client.get_bucket_stat(cold_bucket_2) != (0, 0):
            raise Exception("Bucket for cold data is not empty")

        self.create_table(table_path2)
        self.set_compaction(table_path2, 1500000)

        self.set_tiering(table_path2, cold_bucket_2)

        self.bulk_upsert(table_path2)
        self.wait_tier_data(table_path2, cold_bucket_2)
        self.validate_portion_tier_size(table_path2, 0.5 * 1500000, 2 * 1500000)
