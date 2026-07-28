import logging

from .base import TllTieringTestBase
from ydb.tests.olap.common.column_table_helper import ColumnTableHelper
from ydb.tests.library.test_meta import link_test_case

logger = logging.getLogger(__name__)


class TestDropTableS3(TllTieringTestBase):

    row_count = 10 ** 5
    rows_in_upsert = 10 ** 4
    days_to_cool = 1000

    @link_test_case("#45898")
    def test_drop_table_deletes_data_from_s3(self):
        ''' Implements https://github.com/ydb-platform/ydb/issues/45898 '''
        test_name = "drop_table_s3"
        cold_bucket = "cold_drop"
        test_dir = f"{self.ydb_client.database}/{test_name}"
        table_path = f"{test_dir}/table"
        secret_prefix = test_name
        access_key_id_secret_name = f"{secret_prefix}_key_id"
        access_key_secret_secret_name = f"{secret_prefix}_key_secret"
        cold_eds_path = f"{test_dir}/{cold_bucket}"

        self.s3_client.create_bucket(cold_bucket)

        # Expect empty bucket to avoid unintentional data deletion/modification
        if self.s3_client.get_bucket_stat(cold_bucket) != (0, 0):
            raise Exception("Bucket for cold data is not empty")

        self.ydb_client.query(f"""
            CREATE TABLE `{table_path}` (
                ts Timestamp NOT NULL,
                s String,
                val Uint64,
                PRIMARY KEY(ts),
            )
            WITH (STORE = COLUMN)
            """)

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

        table = ColumnTableHelper(self.ydb_client, table_path)
        table.set_fast_compaction()

        cur_rows = 0
        while cur_rows < self.row_count:
            self.ydb_client.query("""
                $row_count = %i;
                $from_us = CAST(Timestamp('2010-01-01T00:00:00.000000Z') as Uint64);
                $to_us = CAST(Timestamp('2020-01-01T00:00:00.000000Z') as Uint64);
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
            """ % (min(self.row_count - cur_rows, self.rows_in_upsert), table_path))
            cur_rows = table.get_row_count()
            logger.info(f"{cur_rows} rows inserted in total, portions: {table.get_portion_stat_by_tier()}, blobs: {table.get_blob_stat_by_tier()}")

        assert table.portions_actualized_in_sys(), ".sys reports incorrect data portions"

        stmt = f"""
            ALTER TABLE `{table_path}` SET (TTL =
                Interval("P{self.days_to_cool}D") TO EXTERNAL DATA SOURCE `{cold_eds_path}`
                ON ts
            )
        """
        logger.info(stmt)
        self.ydb_client.query(stmt)

        def data_evicted_to_s3():
            bucket_stat = self.s3_client.get_bucket_stat(cold_bucket)
            logger.info(
                f"portions: {table.get_portion_stat_by_tier()}, blobs: {table.get_blob_stat_by_tier()}, cold bucket stat: {bucket_stat}")
            return bucket_stat[0] != 0

        assert self.wait_for(data_evicted_to_s3, 300), "Data eviction has not been started"

        stmt = f"DROP TABLE `{table_path}`"
        logger.info(stmt)
        self.ydb_client.query(stmt)

        def data_deleted_from_bucket():
            bucket_stat = self.s3_client.get_bucket_stat(cold_bucket)
            logger.info(f"cold bucket stat: {bucket_stat}")
            return bucket_stat[0] == 0

        assert self.wait_for(data_deleted_from_bucket, 300), \
            f"Data is not deleted from S3 after DROP TABLE, bucket stat: {self.s3_client.get_bucket_stat(cold_bucket)}"
