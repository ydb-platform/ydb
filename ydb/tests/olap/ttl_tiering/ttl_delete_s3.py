import time
import logging
from .base import TllTieringTestBase, ColumnTableHelper

logger = logging.getLogger(__name__)


class TestDeleteS3Ttl(TllTieringTestBase):
    ''' Implements https://github.com/ydb-platform/ydb/issues/13467 '''

    row_count = 10 ** 7
    single_upsert_row_count = 10 ** 6
    days_to_cool = 1000
    days_to_freeze = 3000

    @classmethod
    def setup_class(cls):
        super(TestDeleteS3Ttl, cls).setup_class()

    def get_row_count_by_date(self, table_path: str, past_days: int) -> int:
        return self.ydb_client.query(f"SELECT count(*) as Rows from `{table_path}` WHERE ts < CurrentUtcTimestamp() - DateTime::IntervalFromDays({past_days})")[0].rows[0]["Rows"]

    def prepare_table(self, test_name, cold_bucket, frozen_bucket):
        test_dir = f"{self.ydb_client.database}/{test_name}"
        self.table_path = f"{test_dir}/table"
        secret_prefix = test_name
        access_key_id_secret_name = f"{secret_prefix}_key_id"
        access_key_secret_secret_name = f"{secret_prefix}_key_secret"
        cold_eds_path = f"{test_dir}/{cold_bucket}"
        frozen_eds_path = f"{test_dir}/{frozen_bucket}"

        self.s3_client.create_bucket(cold_bucket)
        self.s3_client.create_bucket(frozen_bucket)

        # Expect empty buckets to avoid unintentional data deletion/modification
        if self.s3_client.get_bucket_stat(cold_bucket) != (0, 0):
            raise Exception("Bucket for cold data is not empty")
        if self.s3_client.get_bucket_stat(frozen_bucket) != (0, 0):
            raise Exception("Bucket for frozen data is not empty")

        self.ydb_client.query(f"""
            CREATE TABLE `{self.table_path}` (
                ts Timestamp NOT NULL,
                s String,
                val Uint64,
                PRIMARY KEY(ts),
            )
            WITH (STORE = COLUMN)
            """
        )

        logger.info(f"Table {self.table_path} created")

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
        self.table = ColumnTableHelper(self.ydb_client, self.table_path)

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
            """ % (min(self.row_count - cur_rows, self.single_upsert_row_count), self.table_path))
            cur_rows = self.table.get_row_count()
            logger.info(f"{cur_rows} rows inserted in total, portions: {self.table.get_portion_stat_by_tier()}, blobs: {self.table.get_blob_stat_by_tier()}")

        logger.info(f"Rows older than {self.days_to_cool} days: {self.get_row_count_by_date(self.table_path, self.days_to_cool)}")
        logger.info(f"Rows older than {self.days_to_freeze} days: {self.get_row_count_by_date(self.table_path, self.days_to_freeze)}")

        def portions_actualized_in_sys():
            portions = self.table.get_portion_stat_by_tier()
            logger.info(f"portions: {portions}, blobs: {self.table.get_blob_stat_by_tier()}")
            if len(portions) != 1 or "__DEFAULT" not in portions:
                raise Exception("Data not in __DEFAULT teir")
            return self.row_count <= portions["__DEFAULT"]["Rows"]

        if not self.wait_for(lambda: portions_actualized_in_sys(), 120):
            raise Exception(".sys reports incorrect data portions")

        t0 = time.time()
        stmt = f"""
            ALTER TABLE `{self.table_path}` SET (TTL =
                Interval("P{self.days_to_cool}D") TO EXTERNAL DATA SOURCE `{cold_eds_path}`,
                Interval("P{self.days_to_freeze}D") TO EXTERNAL DATA SOURCE `{frozen_eds_path}`
                ON ts
            )
        """
        logger.info(stmt)
        self.ydb_client.query(stmt)
        logger.info(f"TTL set in {time.time() - t0} seconds")

        def data_distributes_across_tiers():
            cold_bucket_stat = self.s3_client.get_bucket_stat(cold_bucket)
            frozen_bucket_stat = self.s3_client.get_bucket_stat(frozen_bucket)
            logger.info(
                f"portions: {self.table.get_portion_stat_by_tier()}, blobs: {self.table.get_blob_stat_by_tier()}, cold bucket stat: {cold_bucket_stat}, frozen bucket stat: {frozen_bucket_stat}"
            )
            # TODO FIXME
            # We can not expect proper distribution of data across tiers due to https://github.com/ydb-platform/ydb/issues/13525
            # So we wait until some data appears in any bucket
            return cold_bucket_stat[0] != 0 or frozen_bucket_stat[0] != 0

        if not self.wait_for(lambda: data_distributes_across_tiers(), 600):
            raise Exception("Data eviction has not been started")

    # TODO FIXME after https://github.com/ydb-platform/ydb/issues/13523
    def data_deleted_from_buckets(self, cold_bucket, frozen_bucket):
        cold_bucket_stat = self.s3_client.get_bucket_stat(cold_bucket)
        frozen_bucket_stat = self.s3_client.get_bucket_stat(frozen_bucket)
        logger.info(
            f"portions: {self.table.get_portion_stat_by_tier()}, blobs: {self.table.get_blob_stat_by_tier()}, cold bucket stat: {cold_bucket_stat}, frozen bucket stat: {frozen_bucket_stat}")
        return cold_bucket_stat[0] == 0 and frozen_bucket_stat[0] == 0

    def test_delete_s3_ttl(self):
        self.prepare_table('delete_s3_ttl', 'cold', 'frozen')
        t0 = time.time()
        stmt = f"""
            ALTER TABLE `{self.table_path}` SET (TTL =
                Interval("P{self.days_to_cool}D")
                ON ts
            )
        """
        logger.info(stmt)
        self.ydb_client.query(stmt)
        logger.info(f"TTL set in {time.time() - t0} seconds")

        if not self.wait_for(lambda: self.data_deleted_from_buckets('cold', 'frozen'), 120):
            # raise Exception("not all data deleted") TODO FIXME after https://github.com/ydb-platform/ydb/issues/13535
            pass

    def test_delete_s3_tiering(self):
        self.prepare_table('delete_s3_tiering', 'cold_delete', 'frozen_delete')
        stmt = f"""
            DELETE FROM `{self.table_path}`
        """
        logger.info(stmt)
        self.ydb_client.query(stmt)

        if not self.wait_for(lambda: self.data_deleted_from_buckets('cold_delete', 'frozen_delete'), 120):
            # raise Exception("not all data deleted") TODO FIXME after https://github.com/ydb-platform/ydb/issues/13594
            pass
