import time
import logging
from .base import TllTieringTestBase, ColumnTableHelper

logger = logging.getLogger(__name__)


def get_all_rows(answer):
    result = []
    for set in answer:
        result += set.rows
    return result


class TestDeleteS3Ttl(TllTieringTestBase):

    test_name = "delete_s3_ttl"
    row_count = 10 ** 7
    single_upsert_row_count = 10 ** 6
    cold_bucket = "cold"
    frozen_bucket = "frozen"
    days_to_cool = 1000
    days_to_freeze = 3000

    @classmethod
    def setup_class(cls):
        super(TestDeleteS3Ttl, cls).setup_class()
        cls.s3_client.create_bucket(cls.cold_bucket)
        cls.s3_client.create_bucket(cls.frozen_bucket)

    def portions_actualized_in_sys(self, table):
        portions = table.get_portion_stat_by_tier()
        logger.info(f"portions: {portions}, blobs: {table.get_blob_stat_by_tier()}")
        return "__DEFAULT" in portions and self.row_count <= portions["__DEFAULT"]["Rows"]

    def get_row_count_by_date(self, table_path: str, past_days: int) -> int:
        return self.ydb_client.query(f"SELECT count(*) as Rows from `{table_path}` WHERE ts < CurrentUtcTimestamp() - DateTime::IntervalFromDays({past_days})")[0].rows[0]["Rows"]

    def test_data_unchanged_after_ttl_change(self):
        ''' Implements https://github.com/ydb-platform/ydb/issues/13542 '''
        self.row_count = 100000
        single_upsert_row_count = 10000
        test_name = 'test_data_unchanged_after_ttl_change'
        cold_bucket = 'cold_uc'
        frozen_bucket = 'frozen_uc'
        self.s3_client.create_bucket(cold_bucket)
        self.s3_client.create_bucket(frozen_bucket)
        test_dir = f"{self.ydb_client.database}/{test_name}"
        table_path = f"{test_dir}/table"
        secret_prefix = test_name
        access_key_id_secret_name = f"{secret_prefix}_key_id"
        access_key_secret_secret_name = f"{secret_prefix}_key_secret"
        cold_eds_path = f"{test_dir}/{cold_bucket}"
        frozen_eds_path = f"{test_dir}/{frozen_bucket}"

        days_to_medium = 2000
        medium_bucket = 'medium'
        self.s3_client.create_bucket(medium_bucket)
        medium_eds_path = f"{test_dir}/{medium_bucket}"
        # Expect empty buckets to avoid unintentional data deletion/modification

        if self.s3_client.get_bucket_stat(cold_bucket) != (0, 0):
            raise Exception("Bucket for cold data is not empty")
        if self.s3_client.get_bucket_stat(frozen_bucket) != (0, 0):
            raise Exception("Bucket for frozen data is not empty")
        if self.s3_client.get_bucket_stat(medium_bucket) != (0, 0):
            raise Exception("Bucket for medium data is not empty")

        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                ts Timestamp NOT NULL,
                s String,
                val Uint64,
                PRIMARY KEY(ts),
            )
            WITH (STORE = COLUMN)
            """
        )

        logger.info(f"Table {table_path} created")

        self.ydb_client.query(f"CREATE OBJECT {access_key_id_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_id}'")
        self.ydb_client.query(f"CREATE OBJECT {access_key_secret_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_secret}'")

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE `{cold_eds_path}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{self.s3_client.endpoint}/{cold_bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            )
        """)

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE `{frozen_eds_path}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{self.s3_client.endpoint}/{frozen_bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            )
        """)

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE `{medium_eds_path}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{self.s3_client.endpoint}/{medium_bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            )
        """)
        table = ColumnTableHelper(self.ydb_client, table_path)

        cur_rows = 0
        while cur_rows < self.row_count:
            self.ydb_client.query("""
                $row_count = %i;
                $from_us = CAST(Timestamp('2010-01-01T00:00:00.000000Z') as Uint64);
                $to_us = CAST(Timestamp('2030-01-01T00:00:00.000000Z') as Uint64);
                $dt = $to_us - $from_us;
                $k = ((1ul << 64) - 1) / CAST($dt - 1 as Double);
                $rows= ListMap(ListFromRange(0, $row_count), ($i)->{
                    $us = CAST(RandomNumber($i) / $k as Uint64) + $from_us;
                    $ts = Unwrap(CAST($us as Timestamp));
                    return <|
                        ts: $ts,
                        s: 'some date:' || CAST($ts as String),
                        val: $us
                    |>;
                });
                upsert into `%s`
                select * FROM AS_TABLE($rows);
            """ % (min(self.row_count - cur_rows, single_upsert_row_count), table_path))
            cur_rows = table.get_row_count()
            logger.info(f"{cur_rows} rows inserted in total, portions: {table.get_portion_stat_by_tier()}, blobs: {table.get_blob_stat_by_tier()}")

        logger.info(f"Rows older than {self.days_to_cool} days: {self.get_row_count_by_date(table_path, self.days_to_cool)}")
        logger.info(f"Rows older than {self.days_to_freeze} days: {self.get_row_count_by_date(table_path, self.days_to_freeze)}")

        if not self.wait_for(lambda: self.portions_actualized_in_sys(table), 120):
            raise Exception(".sys reports incorrect data portions")

        answer = self.ydb_client.query(f"SELECT * from `{table_path}` ORDER BY ts")
        data = get_all_rows(answer)

        def change_ttl_and_check(days_to_cool, days_to_medium, days_to_freeze):
            t0 = time.time()
            stmt = f"""
                ALTER TABLE `{table_path}` SET (TTL =
                    Interval("P{days_to_cool}D") TO EXTERNAL DATA SOURCE `{cold_eds_path}`,
                    Interval("P{days_to_medium}D") TO EXTERNAL DATA SOURCE `{medium_eds_path}`,
                    Interval("P{days_to_freeze}D") TO EXTERNAL DATA SOURCE `{frozen_eds_path}`
                    ON ts
                )
            """
            logger.info(stmt)
            self.ydb_client.query(stmt)
            logger.info(f"TTL set in {time.time() - t0} seconds")

            def data_distributes_across_tiers():
                cold_bucket_stat = self.s3_client.get_bucket_stat(cold_bucket)
                frozen_bucket_stat = self.s3_client.get_bucket_stat(frozen_bucket)
                medium_bucket_stat = self.s3_client.get_bucket_stat(medium_bucket)
                logger.info(f"portions: {table.get_portion_stat_by_tier()}, blobs: {table.get_blob_stat_by_tier()}, cold bucket stat: {cold_bucket_stat}, frozen bucket stat: {frozen_bucket_stat}")
                # TODO FIXME
                # We can not expect proper distribution of data across tiers due to https://github.com/ydb-platform/ydb/issues/13525
                # So we wait until some data appears in any bucket
                return cold_bucket_stat[0] != 0 or frozen_bucket_stat[0] != 0 or medium_bucket_stat[0] != 0

            if not self.wait_for(lambda: data_distributes_across_tiers(), 120):
                raise Exception("Data eviction has not been started")

            answer1 = self.ydb_client.query(f"SELECT * from `{table_path}` ORDER BY ts")
            data1 = get_all_rows(answer1)
            logger.info("Old record count {} new record count {}".format(len(data), len(data1)))
            if data1 != data:
                raise Exception("Data changed after ttl change, was {} now {}".format(data, data1))

            t0 = time.time()
            stmt = f"""
                ALTER TABLE `{table_path}` SET (TTL =
                    Interval("PT800M") TO EXTERNAL DATA SOURCE `{cold_eds_path}`,
                    Interval("PT850M") TO EXTERNAL DATA SOURCE `{medium_eds_path}`,
                    Interval("PT900M") TO EXTERNAL DATA SOURCE `{frozen_eds_path}`
                    ON ts
                )
            """
            logger.info(stmt)
            self.ydb_client.query(stmt)
            logger.info(f"TTL set in {time.time() - t0} seconds")

            def data_deleted_from_buckets():
                cold_bucket_stat = self.s3_client.get_bucket_stat(cold_bucket)
                medium_bucket_stat = self.s3_client.get_bucket_stat(medium_bucket)
                frozen_bucket_stat = self.s3_client.get_bucket_stat(frozen_bucket)
                logger.info(
                    f"portions: {table.get_portion_stat_by_tier()}, blobs: {table.get_blob_stat_by_tier()}, cold bucket stat: {cold_bucket_stat}, frozen bucket stat: {frozen_bucket_stat}")
                return cold_bucket_stat[0] == 0 and frozen_bucket_stat[0] == 0 and medium_bucket_stat[0] == 0

            if not self.wait_for(lambda: data_deleted_from_buckets(), 300):
                raise Exception("not all data deleted")
                pass
            answer1 = self.ydb_client.query(f"SELECT * from `{table_path}` ORDER BY ts")
            data1 = get_all_rows(answer1)
            logger.info("Old record count {} new record count {}".format(len(data), len(data1)))
            if data1 != data:
                raise Exception("Data changed after ttl change, was {} now {}".format(data, data1))

        change_ttl_and_check(self.days_to_cool, days_to_medium, self.days_to_freeze)

    def test_ttl_delete(self):
        ''' Implements https://github.com/ydb-platform/ydb/issues/13467 '''
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/table"
        secret_prefix = self.test_name
        access_key_id_secret_name = f"{secret_prefix}_key_id"
        access_key_secret_secret_name = f"{secret_prefix}_key_secret"
        cold_eds_path = f"{test_dir}/{self.cold_bucket}"
        frozen_eds_path = f"{test_dir}/{self.frozen_bucket}"

        # Expect empty buckets to avoid unintentional data deletion/modification
        if self.s3_client.get_bucket_stat(self.cold_bucket) != (0, 0):
            raise Exception("Bucket for cold data is not empty")
        if self.s3_client.get_bucket_stat(self.frozen_bucket) != (0, 0):
            raise Exception("Bucket for frozen data is not empty")

        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                ts Timestamp NOT NULL,
                s String,
                val Uint64,
                PRIMARY KEY(ts),
            )
            WITH (STORE = COLUMN)
            """
        )

        logger.info(f"Table {table_path} created")

        self.ydb_client.query(f"CREATE OBJECT {access_key_id_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_id}'")
        self.ydb_client.query(f"CREATE OBJECT {access_key_secret_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_secret}'")

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE `{cold_eds_path}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{self.s3_client.endpoint}/{self.cold_bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            )
        """)

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE `{frozen_eds_path}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{self.s3_client.endpoint}/{self.frozen_bucket}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
                AWS_REGION="{self.s3_client.region}"
            )
        """)
        table = ColumnTableHelper(self.ydb_client, table_path)

        cur_rows = 0
        while cur_rows < self.row_count:
            self.ydb_client.query("""
                $row_count = %i;
                $from_us = CAST(Timestamp('2010-01-01T00:00:00.000000Z') as Uint64);
                $to_us = CAST(Timestamp('2030-01-01T00:00:00.000000Z') as Uint64);
                $dt = $to_us - $from_us;
                $k = ((1ul << 64) - 1) / CAST($dt - 1 as Double);
                $rows= ListMap(ListFromRange(0, $row_count), ($i)->{
                    $us = CAST(RandomNumber($i) / $k as Uint64) + $from_us;
                    $ts = Unwrap(CAST($us as Timestamp));
                    return <|
                        ts: $ts,
                        s: 'some date:' || CAST($ts as String),
                        val: $us
                    |>;
                });
                upsert into `%s`
                select * FROM AS_TABLE($rows);
            """ % (min(self.row_count - cur_rows, self.single_upsert_row_count), table_path))
            cur_rows = table.get_row_count()
            logger.info(f"{cur_rows} rows inserted in total, portions: {table.get_portion_stat_by_tier()}, blobs: {table.get_blob_stat_by_tier()}")

        logger.info(f"Rows older than {self.days_to_cool} days: {self.get_row_count_by_date(table_path, self.days_to_cool)}")
        logger.info(f"Rows older than {self.days_to_freeze} days: {self.get_row_count_by_date(table_path, self.days_to_freeze)}")

        if not self.wait_for(lambda: self.portions_actualized_in_sys(table), 120):
            raise Exception(".sys reports incorrect data portions")

        t0 = time.time()
        stmt = f"""
            ALTER TABLE `{table_path}` SET (TTL =
                Interval("P{self.days_to_cool}D") TO EXTERNAL DATA SOURCE `{cold_eds_path}`,
                Interval("P{self.days_to_freeze}D") TO EXTERNAL DATA SOURCE `{frozen_eds_path}`
                ON ts
            )
        """
        logger.info(stmt)
        self.ydb_client.query(stmt)
        logger.info(f"TTL set in {time.time() - t0} seconds")

        def data_distributes_across_tiers():
            cold_bucket_stat = self.s3_client.get_bucket_stat(self.cold_bucket)
            frozen_bucket_stat = self.s3_client.get_bucket_stat(self.frozen_bucket)
            logger.info(f"portions: {table.get_portion_stat_by_tier()}, blobs: {table.get_blob_stat_by_tier()}, cold bucket stat: {cold_bucket_stat}, frozen bucket stat: {frozen_bucket_stat}")
            # TODO FIXME
            # We can not expect proper distribution of data across tiers due to https://github.com/ydb-platform/ydb/issues/13525
            # So we wait until some data appears in any bucket
            return cold_bucket_stat[0] != 0 or frozen_bucket_stat[0] != 0

        if not self.wait_for(lambda: data_distributes_across_tiers(), 600):
            raise Exception("Data eviction has not been started")

        t0 = time.time()
        stmt = f"""
            ALTER TABLE `{table_path}` SET (TTL =
                Interval("P{self.days_to_cool}D")
                ON ts
            )
        """
        logger.info(stmt)
        self.ydb_client.query(stmt)
        logger.info(f"TTL set in {time.time() - t0} seconds")

        def data_deleted_from_buckets():
            cold_bucket_stat = self.s3_client.get_bucket_stat(self.cold_bucket)
            frozen_bucket_stat = self.s3_client.get_bucket_stat(self.frozen_bucket)
            logger.info(
                f"portions: {table.get_portion_stat_by_tier()}, blobs: {table.get_blob_stat_by_tier()}, cold bucket stat: {cold_bucket_stat}, frozen bucket stat: {frozen_bucket_stat}")
            return cold_bucket_stat[0] == 0 and frozen_bucket_stat[0] == 0

        if not self.wait_for(lambda: data_deleted_from_buckets(), 300):
            raise Exception("not all data deleted")
            pass
