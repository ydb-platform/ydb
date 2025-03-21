import time
import logging
from .base import TllTieringTestBase
from ydb.tests.olap.common.column_table_helper import ColumnTableHelper
from ydb.tests.library.common.helpers import plain_or_under_sanitizer

from ydb.tests.library.test_meta import link_test_case

logger = logging.getLogger(__name__)


class TestDataMigrationWhenAlterTtl(TllTieringTestBase):
    test_name = "data_migration_when_alter_tier"
    row_count = 10**6
    single_upsert_row_count = 10**5
    bucket1 = "bucket1"
    bucket2 = "bucket2"

    @classmethod
    def setup_class(cls):
        super(TestDataMigrationWhenAlterTtl, cls).setup_class()
        cls.s3_client.create_bucket(cls.bucket1)
        cls.s3_client.create_bucket(cls.bucket2)

    def get_row_count_by_minute(self, table_path: str, past_minutes: int) -> int:
        return self.ydb_client.query(
            f"SELECT count(*) as Rows from `{table_path}` WHERE timestamp < CurrentUtcTimestamp() - DateTime::IntervalFromMinutes({past_minutes})"
        )[0].rows[0]["Rows"]

    @link_test_case("#13466")
    def test(self):
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/table"
        secret_prefix = self.test_name
        access_key_id_secret_name = f"{secret_prefix}_key_id"
        access_key_secret_secret_name = f"{secret_prefix}_key_secret"
        bucket1_path = f"{test_dir}/{self.bucket1}"
        bucket2_path = f"{test_dir}/{self.bucket2}"
        minutes_to_bucket1 = 2
        minutes_to_bucket2 = 5

        # Expect empty buckets to avoid unintentional data deletion/modification
        if self.s3_client.get_bucket_stat(self.bucket1) != (0, 0):
            raise Exception("Bucket for bucket1 data is not empty")
        if self.s3_client.get_bucket_stat(self.bucket2) != (0, 0):
            raise Exception("Bucket for bucket2 data is not empty")

        self.ydb_client.query(
            f"CREATE OBJECT {access_key_id_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_id}'"
        )
        self.ydb_client.query(
            f"CREATE OBJECT {access_key_secret_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_secret}'"
        )

        # Step 1
        self.ydb_client.query(
            f"""
            CREATE EXTERNAL DATA SOURCE `{bucket1_path}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{self.s3_client.endpoint}/{self.bucket1}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            )
        """
        )

        self.ydb_client.query(
            f"""
            CREATE EXTERNAL DATA SOURCE `{bucket2_path}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{self.s3_client.endpoint}/{self.bucket2}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            )
        """
        )

        # Step 2
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                timestamp Timestamp NOT NULL,
                value Uint64,
                data String,
                PRIMARY KEY(timestamp),
            )
            WITH (STORE = COLUMN)
            """
        )

        logger.info(f"Table {table_path} created")

        table = ColumnTableHelper(self.ydb_client, table_path)

        cur_rows = 0
        while cur_rows < self.row_count:
            self.ydb_client.query(
                """
                $row_count = %i;
                $prev_index = %i;
                $from_us = CAST(Timestamp('2010-01-01T00:00:00.000000Z') as Uint64);
                $rows= ListMap(ListFromRange(0, $row_count), ($i)->{
                    $us = $from_us + $i + $prev_index;
                    $ts = Unwrap(CAST($us as Timestamp));
                    return <|
                        timestamp: $ts,
                        value: $us,
                        data: 'some date:' || CAST($ts as String)
                    |>;
                });
                upsert into `%s`
                select * FROM AS_TABLE($rows);
            """
                % (
                    min(self.row_count - cur_rows, self.single_upsert_row_count),
                    cur_rows,
                    table_path,
                )
            )
            cur_rows = table.get_row_count()
            logger.info(
                f"{cur_rows} rows inserted in total, portions: {table.get_portion_stat_by_tier()}, blobs: {table.get_blob_stat_by_tier()}"
            )

        rows_older_than_bucket2 = self.get_row_count_by_minute(table_path, minutes_to_bucket2)
        logger.info(f"Rows older than {minutes_to_bucket2} minutes: {rows_older_than_bucket2}")
        assert rows_older_than_bucket2 == self.row_count

        if not self.wait_for(lambda: len(table.get_portion_stat_by_tier()) != 0, plain_or_under_sanitizer(60, 120)):
            raise Exception("portion count equal zero after insert data")

        def portions_actualized_in_sys():
            portions = table.get_portion_stat_by_tier()
            logger.info(f"portions: {portions}, blobs: {table.get_blob_stat_by_tier()}")
            if len(portions) != 1 or "__DEFAULT" not in portions:
                raise Exception("Data not in __DEFAULT teir")
            return self.row_count <= portions["__DEFAULT"]["Rows"]

        if not self.wait_for(lambda: portions_actualized_in_sys(), plain_or_under_sanitizer(120, 240)):
            raise Exception(".sys reports incorrect data portions")

        # Step 4
        t0 = time.time()
        stmt = f"""
            ALTER TABLE `{table_path}` SET (TTL =
                Interval("PT{minutes_to_bucket1}M") TO EXTERNAL DATA SOURCE `{bucket1_path}`
                ON timestamp
            )
        """
        logging.info(stmt)
        self.ydb_client.query(stmt)
        logging.info(f"TTL set in {time.time() - t0} seconds")

        def get_rows_in_portion(portion_name: str):
            portions_stat = table.get_portion_stat_by_tier()
            logger.info(f"portions: {portions_stat}, blobs: {table.get_blob_stat_by_tier()}")
            if portion_name in portions_stat:
                return portions_stat[portion_name]['Rows']
            return None

        def bucket_is_not_empty(bucket_name):
            bucket_stat = self.s3_client.get_bucket_stat(bucket_name)
            logger.info(f"bucket: {bucket_name} stat: {bucket_stat}")
            return bucket_stat[0] != 0 and bucket_stat[1] != 0

        # Step 5
        if not self.wait_for(
            lambda: get_rows_in_portion(bucket1_path) == self.row_count
            and bucket_is_not_empty(self.bucket1)
            and not bucket_is_not_empty(self.bucket2),
            plain_or_under_sanitizer(600, 1200),
        ):
            raise Exception("Data eviction has not been started")

        # Step 6
        t0 = time.time()
        stmt = f"""
            ALTER TABLE `{table_path}` SET (TTL =
                Interval("PT{minutes_to_bucket2}M") TO EXTERNAL DATA SOURCE `{bucket2_path}`
                ON timestamp
            )
        """
        logging.info(stmt)
        self.ydb_client.query(stmt)
        logging.info(f"TTL set in {time.time() - t0} seconds")

        # Step 7
        if not self.wait_for(
            lambda: get_rows_in_portion(bucket2_path) == self.row_count and bucket_is_not_empty(self.bucket2),
            plain_or_under_sanitizer(600, 1200),
        ):
            raise Exception("Data eviction has not been started")

        # Wait until bucket1 is empty
        if not self.wait_for(
            lambda: not bucket_is_not_empty(self.bucket1),
            plain_or_under_sanitizer(120, 240),  # TODO: change wait time use config "PeriodicWakeupActivationPeriod"
        ):
            raise Exception("Bucket1 is not empty")

        # Step 8
        t0 = time.time()
        stmt = f"""
            ALTER TABLE `{table_path}` SET (TTL =
                Interval("P10000D") TO EXTERNAL DATA SOURCE `{bucket1_path}`
                ON timestamp
            )
        """
        logging.info(stmt)
        self.ydb_client.query(stmt)
        logging.info(f"TTL set in {time.time() - t0} seconds")

        # Step 9
        if not self.wait_for(
            lambda: get_rows_in_portion("__DEFAULT") == self.row_count,
            plain_or_under_sanitizer(600, 1200),
        ):
            raise Exception("Data eviction has not been started")

        # Wait until buckets are empty
        if not self.wait_for(
            lambda: not bucket_is_not_empty(self.bucket1) and not bucket_is_not_empty(self.bucket2),
            plain_or_under_sanitizer(120, 240),  # TODO: change wait time use config "PeriodicWakeupActivationPeriod"
        ):
            raise Exception("Buckets are not empty")
