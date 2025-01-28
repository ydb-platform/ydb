import argparse
import ydb
import time
import boto3

class YdbClient:
    def __init__(self, endpoint, database):
        self.driver = ydb.Driver(endpoint=endpoint, database=database, oauth=None)
        self.database = database
        self.session_pool = ydb.QuerySessionPool(self.driver)

    def wait_connection(self, timeout=5):
        self.driver.wait(timeout, fail_fast=True)

    def query(self, statement):
            return self.session_pool.execute_with_retries(statement)
    
class S3Client:
    def __init__(self, endpoint, region, key_id, key_secret):
        self.endpoint = endpoint
        self.region = region
        self.key_id = key_id
        self.key_secret = key_secret

        session = boto3.session.Session()
        self.s3 = session.resource(
            service_name = "s3",
            aws_access_key_id = key_id,
            aws_secret_access_key = key_secret,
            region_name = region,
            endpoint_url = endpoint
        )

    def get_bucket_stat(self, bucket_name: str) -> (int, int):
        bucket = self.s3.Bucket(bucket_name)
        count = 0
        size = 0
        for obj in bucket.objects.all():
            count += 1
            size += obj.size 
        return (count, size)


def wait_for(condition_func, timeout_seconds):
    t0 = time.time()
    while time.time() - t0 < timeout_seconds:
        if condition_func():
            return True
        time.sleep(1)
    return False

def run_test_delete_s3_ttl(ydb_client: YdbClient, path_prefix: str, row_count: int, rows_in_upsert: int,
            s3_client: S3Client, cold_bucket: str, frozen_bucket: str):
    ''' Implements https://github.com/ydb-platform/ydb/issues/13467 '''
    test_name = "delete_tiering"
    test_dir = f"{ydb_client.database}/{path_prefix}/{test_name}"
    table_path = f"{test_dir}/table"
    secret_prefix = f"{path_prefix}_{test_name}"
    access_key_id_secret_name = f"{secret_prefix}_key_id"
    access_key_secret_secret_name = f"{secret_prefix}_key_secret"
    cold_eds_path = f"{test_dir}/cold"
    frozen_eds_path =  f"{test_dir}/frozen"
    days_to_cool = 1000
    days_to_froze = 3000

    # Expect empty buckets to avoid unintentional data deletion/modification
    if s3_client.get_bucket_stat(cold_bucket) != (0, 0):
        raise Exception("Bucket for cold data is not empty")
    if s3_client.get_bucket_stat(frozen_bucket) != (0, 0):
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
            ydb_client.query(s)
            print("OK")
        except:
            print("FAIL")


    ydb_client.query(f"""
        CREATE TABLE `{table_path}` (
            ts Timestamp NOT NULL,
            s String,
            val Uint64,
            PRIMARY KEY(ts),
        )
        WITH (STORE = COLUMN)
        """
    )

    ydb_client.query(f"CREATE OBJECT {access_key_id_secret_name} (TYPE SECRET) WITH value='{s3_client.key_id}'")
    ydb_client.query(f"CREATE OBJECT {access_key_secret_secret_name} (TYPE SECRET) WITH value='{s3_client.key_secret}'") 

    ydb_client.query(f"""
        CREATE EXTERNAL DATA SOURCE `{cold_eds_path}` WITH (
            SOURCE_TYPE="ObjectStorage",
            LOCATION="{s3_client.endpoint}/{cold_bucket}",
            AUTH_METHOD="AWS",
            AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
            AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
            AWS_REGION="{s3_client.region}"
        )
    """)


    ydb_client.query(f"""
        CREATE EXTERNAL DATA SOURCE `{frozen_eds_path}` WITH (
            SOURCE_TYPE="ObjectStorage",
            LOCATION="{s3_client.endpoint}/{frozen_bucket}",
            AUTH_METHOD="AWS",
            AWS_ACCESS_KEY_ID_SECRET_NAME="{access_key_id_secret_name}",
            AWS_SECRET_ACCESS_KEY_SECRET_NAME="{access_key_secret_secret_name}",
            AWS_REGION="{s3_client.region}"
        )
    """)

    def get_row_count() -> int:
        return ydb_client.query(f"SELECT count(*) as Rows from `{table_path}`")[0].rows[0]["Rows"]

    def get_row_count_by_date(past_days: int) -> int:
        return ydb_client.query(f"SELECT count(*) as Rows from `{table_path}` WHERE ts < CurrentUtcTimestamp() - DateTime::IntervalFromDays({past_days})")[0].rows[0]["Rows"]

    def get_portion_count() -> int:
        return ydb_client.query(f"select count(*) as Rows from `{table_path}/.sys/primary_index_portion_stats`")[0].rows[0]["Rows"]


    cur_rows = 0
    while cur_rows < row_count:
        ydb_client.query("""
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
         results = ydb_client.query(f"select TierName, sum(Rows) as Rows from `{table_path}/.sys/primary_index_portion_stats` GROUP BY TierName")
         return {row["TierName"]: row["Rows"] for result_set in results for row in result_set.rows}

    print(f"After inserting: {get_rows_by_tier()}, portions: {get_portion_count()}")
    print(f"Rows older than {days_to_cool} days: {get_row_count_by_date(days_to_cool)}")
    print(f"Rows older than {days_to_froze} days: {get_row_count_by_date(days_to_froze)}")

    def portions_actualized_in_sys():
        rows_by_tier = get_rows_by_tier()
        print(f"rows by tar: {rows_by_tier}, portions: {get_portion_count()}")
        if len(rows_by_tier) != 1:
            raise Exception("Data not in __DEFAULT teir")
        return row_count <= rows_by_tier["__DEFAULT"]

    if not wait_for(lambda: portions_actualized_in_sys(), 60):
        raise Exception(f".sys reports incorrect data portions")

    t0 = time.time()
    stmt = f"""
        ALTER TABLE `{table_path}` SET (TTL =
            Interval("P{days_to_cool}D") TO EXTERNAL DATA SOURCE `{cold_eds_path}`,
            Interval("P{days_to_froze}D") TO EXTERNAL DATA SOURCE `{frozen_eds_path}`
            ON ts
        )
    """
    print(stmt)
    ydb_client.query(stmt)
    print(f"TTL set in {time.time() - t0} seconds")

    def data_distributes_across_tiers():
        rows_by_tier = get_rows_by_tier()
        cold_bucket_stat = s3_client.get_bucket_stat(cold_bucket)
        frozen_bucket_stat = s3_client.get_bucket_stat(frozen_bucket)
        print(f"rows by tier: {rows_by_tier}, portions: {get_portion_count()}, cold bucket stat: {cold_bucket_stat}, frozen bucket stat: {frozen_bucket_stat}")
        # TODO FIXME
        # We can not expect proper distribution of data across tiers due to https://github.com/ydb-platform/ydb/issues/13525
        # So we wait until some data appears in any bucket
        return cold_bucket_stat[0] != 0 or frozen_bucket_stat[0] != 0

    if not wait_for(lambda: data_distributes_across_tiers(), 600):
        raise Exception("Data eviction has not been started")

    t0 = time.time()
    stmt = f"""
        DELETE FROM `{table_path}`
    """
    print(stmt)
    ydb_client.query(stmt)

    # TODO FIXME after https://github.com/ydb-platform/ydb/issues/13523
    def data_deleted_from_buckets():
        rows_by_tier = get_rows_by_tier()
        cold_bucket_stat = s3_client.get_bucket_stat(cold_bucket)
        frozen_bucket_stat = s3_client.get_bucket_stat(frozen_bucket)
        print(f"rows by tier: {rows_by_tier}, portions: {get_portion_count()}, cold bucket stat: {cold_bucket_stat}, frozen bucket stat: {frozen_bucket_stat}")
        return cold_bucket_stat[0] == 0 and frozen_bucket_stat[0] == 0

    if wait_for(lambda: data_deleted_from_buckets(), 120):
        raise Exception("all data deleted")
    raise Exception("data are not deleted")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Test data deletion by ttl from s3", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--endpoint", default="grpc://localhost:2135", help="YDB endpoint to be used")
    parser.add_argument("--database", default="/Root/test", help="A database to connect")
    parser.add_argument("--prefix", default="olap_tests", help="A path prefix for in database")
    parser.add_argument("--row-count", default=10 ** 5, type=lambda x: int(x), help="Number of rows to be inserted into a table")
    parser.add_argument("--rows-in-upsert", default=10 ** 4, type=lambda x: int(x), help="Number of rows in a single upsert operation")
    parser.add_argument("--s3-endpoint", default="http://localhost:4000", help="S3 endpoint")
    parser.add_argument("--s3-region", default="us-east-1", help="S3 region")
    parser.add_argument("--s3-key-id", default="fake_access_key_id", help="aws_access_key_id")
    parser.add_argument("--s3-key-secret", default="fake_secret_access_key", help="aws_secret_access_key")
    parser.add_argument("--bucket-cold", default="cold", help="bucket for cooled data")
    parser.add_argument("--bucket-frozen", default="frozen", help="bucket for frozen data")
    args = parser.parse_args()

    ydb_client = YdbClient(args.endpoint, args.database)
    ydb_client.wait_connection()

    s3_client = S3Client(args.s3_endpoint, args.s3_region, args.s3_key_id, args.s3_key_secret)
    session = boto3.session.Session()
    s3client = session.client(
             service_name='s3',
             aws_access_key_id = "fake_access_key_id",
             aws_secret_access_key = "fake_secret_access_key",
             region_name = "us-east-1",
             endpoint_url=args.s3_endpoint)

    s3client.create_bucket(Bucket = "cold")
    s3client.create_bucket(Bucket = "frozen")

    run_test_delete_s3_ttl(ydb_client, args.prefix, args.row_count, args.rows_in_upsert, s3_client, args.bucket_cold, args.bucket_frozen)
