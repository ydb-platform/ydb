import sys
import time
import logging

from .base import TllTieringTestBase
from ydb.tests.olap.lib.utils import get_external_param
from ydb.tests.olap.common.column_table_helper import ColumnTableHelper


logger = logging.getLogger(__name__)


class TestTierDelete(TllTieringTestBase):
    test_name = "test_delete_s3_ttl"

    @classmethod
    def setup_class(cls):
        super(TestTierDelete, cls).setup_class()
        pass

    def test_delete_s3_ttl(self):
        path_prefix = get_external_param("prefix", "olap_tests")
        row_count = int(get_external_param("row-count", 10 ** 5))
        rows_in_upsert = int(get_external_param("rows-in-upsert", 10 ** 4))
        cold_bucket = get_external_param("bucket-cold", "cold")
        frozen_bucket = get_external_param("bucket-frozen", "frozen")

        """
        session = boto3.session.Session()
        s3client = session.client(
                 service_name='s3',
                 aws_access_key_id = s3_key_id,
                 aws_secret_access_key = s3_key_secret,
                 region_name = s3_region,
                 endpoint_url=s3_endpoint)
        """
        self.s3_client.create_bucket("cold")
        self.s3_client.create_bucket("frozen")

        ''' Implements https://github.com/ydb-platform/ydb/issues/13467 '''
        test_name = "delete_tiering"
        test_dir = f"{self.ydb_client.database}/{path_prefix}/{test_name}"
        table_path = f"{test_dir}/table"
        secret_prefix = f"{path_prefix}_{test_name}"
        access_key_id_secret_name = f"{secret_prefix}_key_id"
        access_key_secret_secret_name = f"{secret_prefix}_key_secret"
        cold_eds_path = f"{test_dir}/cold"
        frozen_eds_path = f"{test_dir}/frozen"
        days_to_cool = 1000
        days_to_froze = 3000

        # Expect empty buckets to avoid unintentional data deletion/modification
        if self.s3_client.get_bucket_stat(cold_bucket) != (0, 0):
            raise Exception("Bucket for cold data is not empty")
        if self.s3_client.get_bucket_stat(frozen_bucket) != (0, 0):
            raise Exception("Bucket for frozen data is not empty")

        delete_resource_statements = [
            f"DROP TABLE `{table_path}`",
            f"DROP EXTERNAL DATA SOURCE `{cold_eds_path}`",
            f"DROP EXTERNAL DATA SOURCE `{frozen_eds_path}`",
            f"DROP OBJECT {access_key_id_secret_name} (TYPE SECRET)",
            f"DROP OBJECT {access_key_secret_secret_name} (TYPE SECRET)"
        ]

        for s in delete_resource_statements:
            try:
                print(s)
                self.ydb_client.query(s)
                print("OK")
            except Exception:
                print("FAIL")

        self.ydb_client.query(f"""
            CREATE TABLE `{table_path}` (
                ts Timestamp NOT NULL,
                s String,
                val Uint64,
                PRIMARY KEY(ts),
            )
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT=2)
            """)

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

        def get_row_count() -> int:
            return self.ydb_client.query(f"SELECT count(*) as Rows from `{table_path}`")[0].rows[0]["Rows"]

        def get_row_count_by_date(past_days: int) -> int:
            return self.ydb_client.query(f"SELECT count(*) as Rows from `{table_path}` WHERE ts < CurrentUtcTimestamp() - DateTime::IntervalFromDays({past_days})")[0].rows[0]["Rows"]

        def get_portion_count() -> int:
            return self.ydb_client.query(f"select count(*) as Rows from `{table_path}/.sys/primary_index_portion_stats`")[0].rows[0]["Rows"]

        cur_rows = 0
        while cur_rows < row_count:
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
            """ % (min(row_count - cur_rows, rows_in_upsert), table_path))
            cur_rows = get_row_count()
            print(f"{cur_rows} rows inserted in total, portions: {get_portion_count()}")

        def get_rows_by_tier() -> dict[str, int]:
            results = self.ydb_client.query(f"select TierName, sum(Rows) as Rows from `{table_path}/.sys/primary_index_portion_stats` GROUP BY TierName")
            return {row["TierName"]: row["Rows"] for result_set in results for row in result_set.rows}

        print(f"After inserting: {get_rows_by_tier()}, portions: {get_portion_count()}")
        print(f"Rows older than {days_to_cool} days: {get_row_count_by_date(days_to_cool)}")
        print(f"Rows older than {days_to_froze} days: {get_row_count_by_date(days_to_froze)}")

        def portions_actualized_in_sys():
            rows_by_tier = get_rows_by_tier()
            print(f"rows by tier: {rows_by_tier}, portions: {get_portion_count()}", file=sys.stderr)
            if len(rows_by_tier) != 1:
                return False
            return row_count <= rows_by_tier["__DEFAULT"]

        table = ColumnTableHelper(self.ydb_client, table_path)
        assert table.portions_actualized_in_sys(), ".sys reports incorrect data portions"

        t0 = time.time()
        stmt = f"""
            ALTER TABLE `{table_path}` SET (TTL =
                Interval("P{days_to_cool}D") TO EXTERNAL DATA SOURCE `{cold_eds_path}`,
                Interval("P{days_to_froze}D") TO EXTERNAL DATA SOURCE `{frozen_eds_path}`
                ON ts
            )
        """
        print(stmt)
        self.ydb_client.query(stmt)
        print(f"TTL set in {time.time() - t0} seconds")

        def data_distributes_across_tiers():
            rows_by_tier = get_rows_by_tier()
            cold_bucket_stat = self.s3_client.get_bucket_stat(cold_bucket)
            frozen_bucket_stat = self.s3_client.get_bucket_stat(frozen_bucket)
            print(f"rows by tier: {rows_by_tier}, portions: {get_portion_count()}, cold bucket stat: {cold_bucket_stat}, frozen bucket stat: {frozen_bucket_stat}")
            # TODO FIXME
            # We can not expect proper distribution of data across tiers due to https://github.com/ydb-platform/ydb/issues/13525
            # So we wait until some data appears in any bucket
            return cold_bucket_stat[0] != 0 or frozen_bucket_stat[0] != 0

        if not self.wait_for(lambda: data_distributes_across_tiers(), 120):
            # there is a bug in tiering, after fixing, replace print by next line
            # raise Exception("Data eviction has not been started")
            print("Data eviction has not been started")
        t0 = time.time()
        stmt = f"""
            DELETE FROM `{table_path}`
        """
        print(stmt)
        self.ydb_client.query(stmt)

        # TODO FIXME after https://github.com/ydb-platform/ydb/issues/13523
        def data_deleted_from_buckets():
            rows_by_tier = get_rows_by_tier()
            cold_bucket_stat = self.s3_client.get_bucket_stat(cold_bucket)
            frozen_bucket_stat = self.s3_client.get_bucket_stat(frozen_bucket)
            print(f"rows by tier: {rows_by_tier}, portions: {get_portion_count()}, cold bucket stat: {cold_bucket_stat}, frozen bucket stat: {frozen_bucket_stat}")
            return cold_bucket_stat[0] == 0 and frozen_bucket_stat[0] == 0

        assert self.wait_for(lambda: data_deleted_from_buckets(), 180), "Not all data deleted"
