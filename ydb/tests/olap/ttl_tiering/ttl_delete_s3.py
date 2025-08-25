import time
import logging
from .base import TllTieringTestBase
from ydb.tests.olap.common.column_table_helper import ColumnTableHelper
from ydb.tests.library.test_meta import link_test_case

logger = logging.getLogger(__name__)


class TllDeleteBase(TllTieringTestBase):

    row_count = 10 ** 7
    single_upsert_row_count = 10 ** 6
    days_to_cool = 1000
    days_to_freeze = 3000

    @classmethod
    def get_row_count_by_date(self, table_path: str, past_days: int) -> int:
        return self.ydb_client.query(f"SELECT count(*) as Rows from `{table_path}` WHERE ts < CurrentUtcTimestamp() - DateTime::IntervalFromDays({past_days})")[0].rows[0]["Rows"]


class TestDeleteS3Ttl(TllDeleteBase):

    @classmethod
    def setup_class(cls):
        super(TestDeleteS3Ttl, cls).setup_class()

    def get_aggregated(self, table_path):
        answer = self.ydb_client.query(f"SELECT count(*), sum(val), sum(Digest::Fnv32(s)) from `{table_path}`")
        return [answer[0].rows[0][0], answer[0].rows[0][1], answer[0].rows[0][2]]

    def create_table(self, table_path):
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                ts Timestamp NOT NULL,
                s String,
                val Uint64,
                PRIMARY KEY(ts),
            )
            WITH (STORE = COLUMN, PARTITION_COUNT=2)
            """
        )
        table = ColumnTableHelper(self.ydb_client, table_path)
        logger.info(f"Table {table_path} created")
        return table

    def fill_table(self, table, table_path, single_upsert_row_count, upsert_number):
        for i in range(upsert_number):
            self.ydb_client.query("""
                $current_chunk = %i;
                $row_count = %i;
                $from_us = CAST(Timestamp('2010-01-01T00:00:00.000000Z') as Uint64);
                $to_us = CAST(Timestamp('2030-01-01T00:00:00.000000Z') as Uint64);
                $dt = $to_us - $from_us;
                $chunk_size = CAST($dt as Double) / %i;
                $k = ((1ul << 64) - 1) / $chunk_size;
                $offset = CAST($from_us + $current_chunk * $chunk_size as Uint64);
                $rows = ListMap(ListFromRange(0, $row_count), ($i)->{
                    $us = $offset + CAST(RandomNumber($i) / $k as Uint64);
                    $ts = Unwrap(CAST($us as Timestamp));
                    return <|
                        ts: $ts,
                        s: 'some date:' || CAST($ts as String),
                        val: $us
                    |>;
                });
                upsert into `%s`
                select * FROM AS_TABLE($rows);
            """ % (i, single_upsert_row_count, upsert_number, table_path))
            cur_rows = table.get_row_count()
            logger.info(f"{cur_rows} rows inserted in total, portions: {table.get_portion_stat_by_tier()}, blobs: {table.get_blob_stat_by_tier()}")
        logger.info(f"Table {table_path} filled")

    def init_buckets(self, test_dir, key_id, key_secret, buckets):
        for bucket in buckets :
            self.s3_client.create_bucket(bucket)

            if self.s3_client.get_bucket_stat(bucket) != (0, 0):
                raise Exception("Bucket for cold data is not empty")

            self.ydb_client.query(f"""
                CREATE EXTERNAL DATA SOURCE `{test_dir}/{bucket}` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{self.s3_client.endpoint}/{bucket}",
                    AUTH_METHOD="AWS",
                    AWS_ACCESS_KEY_ID_SECRET_NAME="{key_id}",
                    AWS_SECRET_ACCESS_KEY_SECRET_NAME="{key_secret}",
                    AWS_REGION="{self.s3_client.region}"
                )
            """)

            logger.info(f"Bucket {bucket} inited")

    def set_ttl(self, test_dir, buckets, ttls, table_path):
        t0 = time.time()
        ttl_change_body = ',\n'.join(f"""Interval("{ttl}") TO EXTERNAL DATA SOURCE `{test_dir}/{bucket}`""" for bucket, ttl in zip(buckets, ttls))
        stmt = f"""
            ALTER TABLE `{table_path}` SET (TTL =
                {ttl_change_body}
                ON ts
            )
        """
        logger.info(stmt)
        self.ydb_client.query(stmt)
        logger.info(f"TTL set in {time.time() - t0} seconds")

    def reset_ttl(self, table_path):

        t0 = time.time()
        stmt = f"""
            ALTER TABLE  `{table_path}` RESET (TTL);
        """
        logger.info(stmt)
        self.ydb_client.query(stmt)
        logger.info(f"TTL set in {time.time() - t0} seconds")

    def all_buckets(self, f, buckets, table):
        logger.info(f"portions: {table.get_portion_stat_by_tier()}, blobs: {table.get_blob_stat_by_tier()}")
        not_empty = True
        for bucket in buckets:
            bucket_stat = self.s3_client.get_bucket_stat(bucket)
            logger.info(f"{bucket} bucket stat: {bucket_stat}")
            not_empty = not_empty and f(bucket_stat[0])

        return not_empty

    def wait_rows_stable(self, table_path, expected, attempts=3, timeout=300):
        ok = [False] * attempts
        last = None

        def _cond():
            last = self.get_aggregated(table_path)
            ok.pop(0)
            ok.append(last == expected)
            return all(ok)

        if not self.wait_for(_cond, timeout):
            raise Exception(f"Row count not stable: last = {last}, expected = {expected}")

    def teset_generator(self, test_name, buckets, ttl, single_upsert_row_count, upsert_number):
        test_dir = f"{self.ydb_client.database}/{test_name}"
        table_path = f"{test_dir}/table"
        secret_prefix = test_name
        access_key_id_secret_name = f"{secret_prefix}_key_id"
        access_key_secret_secret_name = f"{secret_prefix}_key_secret"

        self.ydb_client.query(f"CREATE OBJECT {access_key_id_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_id}'")
        self.ydb_client.query(f"CREATE OBJECT {access_key_secret_secret_name} (TYPE SECRET) WITH value='{self.s3_client.key_secret}'")

        table = self.create_table(table_path)
        table.set_fast_compaction()
        self.init_buckets(test_dir, access_key_id_secret_name, access_key_secret_secret_name, buckets)

        self.fill_table(table, table_path, single_upsert_row_count, upsert_number)

        data = self.get_aggregated(table_path)
        logger.info('Aggregated answer {}'.format(data))

        self.set_ttl(test_dir, buckets, ttl, table_path)
        if not self.wait_for(lambda: self.all_buckets(lambda x: x != 0, buckets, table), 300):
            raise Exception("Data eviction has not been started")

        self.wait_rows_stable(table_path, expected=data, attempts=3, timeout=300)

        self.reset_ttl(table_path)

        if not self.wait_for(lambda: self.all_buckets(lambda x: x == 0, buckets, table), 300):
            raise Exception("not all data deleted")

        self.wait_rows_stable(table_path, expected=data, attempts=3, timeout=300)

    @link_test_case("#13542")
    def test_data_unchanged_after_ttl_change(self):
        ''' Implements https://github.com/ydb-platform/ydb/issues/13542 '''

        test_name = 'test_data_unchanged_after_ttl_change'
        buckets = ["cold_uc", "middle_uc", "frozen_uc"]
        ttl = ["P1000D", "P2000D", "P3000D"]
        single_upsert_row_count = 10_000
        upsert_number = 100
        self.teset_generator(test_name, buckets, ttl, single_upsert_row_count, upsert_number)

    # TODO FIXME after https://github.com/ydb-platform/ydb/issues/13523
    def data_deleted_from_buckets(self, cold_bucket, frozen_bucket):
        cold_bucket_stat = self.s3_client.get_bucket_stat(cold_bucket)
        frozen_bucket_stat = self.s3_client.get_bucket_stat(frozen_bucket)
        logger.info(
            f"portions: {self.table.get_portion_stat_by_tier()}, blobs: {self.table.get_blob_stat_by_tier()}, cold bucket stat: {cold_bucket_stat}, frozen bucket stat: {frozen_bucket_stat}")
        return cold_bucket_stat[0] == 0 and frozen_bucket_stat[0] == 0

    def test_delete_s3_tiering(self):
        ''' Implements https://github.com/ydb-platform/ydb/issues/13468 '''
        self.test_name = 'delete_s3_tiering'
        cold_bucket = 'cold_delete'
        frozen_bucket = 'frozen_delete'
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/table"
        secret_prefix = self.test_name
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
        self.table = ColumnTableHelper(self.ydb_client, table_path)

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
            cur_rows = self.table.get_row_count()
            logger.info(f"{cur_rows} rows inserted in total, portions: {self.table.get_portion_stat_by_tier()}, blobs: {self.table.get_blob_stat_by_tier()}")

        logger.info(f"Rows older than {self.days_to_cool} days: {self.get_row_count_by_date(table_path, self.days_to_cool)}")
        logger.info(f"Rows older than {self.days_to_freeze} days: {self.get_row_count_by_date(table_path, self.days_to_freeze)}")

        assert ColumnTableHelper.portions_actualized_in_sys(self.table)

        stmt = f"""
            DELETE FROM `{table_path}`
        """
        logger.info(stmt)
        self.ydb_client.query(stmt)

        assert self.wait_for(lambda: self.data_deleted_from_buckets('cold_delete', 'frozen_delete'), 200), "not all data deleted"


class TestDeleteTtl(TllDeleteBase):

    @classmethod
    def setup_class(cls):
        super(TestDeleteTtl, cls).setup_class()

    @link_test_case("#13467")
    def test_ttl_delete(self):
        ''' Implements https://github.com/ydb-platform/ydb/issues/13467 '''
        self.test_name = 'test_ttl_delete'
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/table"
        cold_bucket = 'cold'
        frozen_bucket = 'frozen'
        secret_prefix = self.test_name
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

        self.ydb_client.query(
            f"""
            ALTER OBJECT `{table_path}` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`l-buckets`)
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
        self.table = ColumnTableHelper(self.ydb_client, table_path)

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
            cur_rows = self.table.get_row_count()
            logger.info(f"{cur_rows} rows inserted in total, portions: {self.table.get_portion_stat_by_tier()}, blobs: {self.table.get_blob_stat_by_tier()}")

        logger.info(f"Rows older than {self.days_to_cool} days: {self.get_row_count_by_date(table_path, self.days_to_cool)}")
        logger.info(f"Rows older than {self.days_to_freeze} days: {self.get_row_count_by_date(table_path, self.days_to_freeze)}")

        assert ColumnTableHelper.portions_actualized_in_sys(self.table)

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
            cold_bucket_stat = self.s3_client.get_bucket_stat(cold_bucket)
            frozen_bucket_stat = self.s3_client.get_bucket_stat(frozen_bucket)
            logger.info(
                f"portions: {self.table.get_portion_stat_by_tier()}, blobs: {self.table.get_blob_stat_by_tier()}, cold bucket stat: {cold_bucket_stat}, frozen bucket stat: {frozen_bucket_stat}"
            )
            # TODO FIXME
            # We can not expect proper distribution of data across tiers due to https://github.com/ydb-platform/ydb/issues/13525
            # So we wait until some data appears in any bucket
            return cold_bucket_stat[0] != 0 or frozen_bucket_stat[0] != 0

        assert self.wait_for(lambda: data_distributes_across_tiers(), 200), "Data eviction has not been started"
