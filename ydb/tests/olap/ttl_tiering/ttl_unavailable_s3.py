import os
import signal
import sys
import time
import logging

from .base import TllTieringTestBase
from ydb.tests.olap.common.column_table_helper import ColumnTableHelper
from ydb.tests.library.test_meta import link_test_case

logger = logging.getLogger(__name__)

ROWS_CHUNK_SIZE = 1000000
ROWS_CHUNKS_COUNT = 10


class TestUnavailableS3(TllTieringTestBase):
    @link_test_case("#13545")
    def test(self):
        bucket_s3_name = "cold"
        bucket_db_path = f"{self.ydb_client.database}/buckets/{bucket_s3_name}"

        self.ydb_client.query("""
            CREATE TABLE table (
                ts Timestamp NOT NULL,
                v String,
                PRIMARY KEY(ts),
            )
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2
            )
        """)

        table = ColumnTableHelper(self.ydb_client, f"{self.ydb_client.database}/table")
        table.set_fast_compaction()

        self.s3_client.create_bucket(bucket_s3_name)

        self.ydb_client.query(f"CREATE OBJECT s3_id (TYPE SECRET) WITH value = '{self.s3_client.key_id}'")
        self.ydb_client.query(f"CREATE OBJECT s3_secret (TYPE SECRET) WITH value = '{self.s3_client.key_secret}'")

        self.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE `{bucket_db_path}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{self.s3_client.endpoint}/{bucket_s3_name}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="s3_id",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="s3_secret",
                AWS_REGION="{self.s3_client.region}"
            )
        """)

        # table = ColumnTableHelper(self.ydb_client, 'table')

        def upsert_chunk(i):
            return self.ydb_client.query(f"""
                $n = {ROWS_CHUNK_SIZE};
                $beg_ul = CAST(Timestamp('2020-01-01T00:00:00.000000Z') as Uint64);
                $end_ul = CAST(Timestamp('2030-01-01T00:00:00.000000Z') as Uint64);
                $int_ul = $end_ul - $beg_ul;
                $step_ul = 100000;
                $rows_list = ListMap(ListFromRange(0, $n), ($j) -> (<|
                    ts: UNWRAP(CAST($beg_ul + $step_ul * {i}ul + CAST(Random($j) * $int_ul AS Uint64) AS Timestamp)),
                    v: "Entry #" || CAST($j AS String)
                |>));

                UPSERT INTO table
                SELECT * FROM AS_TABLE($rows_list);
            """)

        def get_stat():
            return self.s3_client.get_bucket_stat(bucket_s3_name)[0]

        for i in range(0, ROWS_CHUNKS_COUNT // 2):
            upsert_chunk(i)

        self.ydb_client.query(f"""
            ALTER TABLE table SET (TTL =
                Interval("P365D") TO EXTERNAL DATA SOURCE `{bucket_db_path}`
                ON ts
            )
        """)

        assert self.wait_for(get_stat, 30), "initial eviction"

        print("!!! simulating S3 hang up -- sending SIGSTOP", file=sys.stderr)
        os.kill(self.s3_pid, signal.SIGSTOP)

        time.sleep(30)

        print("!!! simulating S3 recovery -- sending SIGCONT", file=sys.stderr)
        os.kill(self.s3_pid, signal.SIGCONT)

        stat_old = get_stat()

        for i in range(ROWS_CHUNKS_COUNT // 2, ROWS_CHUNKS_COUNT):
            upsert_chunk(i)

        assert self.wait_for(lambda: get_stat() != stat_old, 120), "data distribution continuation"
