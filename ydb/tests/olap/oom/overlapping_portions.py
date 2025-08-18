import datetime
import logging
import os
import pytest
import random
import yatest.common
import ydb

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.test_meta import link_test_case
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class TestOverlappingPortions(object):
    test_name = "overlapping_portions"

    @classmethod
    def setup_class(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            column_shard_config={"compaction_enabled": False},
            deduplication_grouped_memory_limiter_config={
                "enabled": True,
                "memory_limit": 1024 * 1024,
                "hard_memory_limit": 1024 * 1024,
            },
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        cls.ydb_client.wait_connection()

    def write_data(
        self,
        table: str,
        timestamp_from_ms: int,
        rows: int,
        value: int = 1,
    ):
        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("ts", ydb.PrimitiveType.Timestamp)
        column_types.add_column("s", ydb.PrimitiveType.String)
        column_types.add_column("val", ydb.PrimitiveType.Uint64)

        chunk_size = 100
        while rows:
            current_chunk_size = min(chunk_size, rows)
            data = [
                {
                    "ts": timestamp_from_ms + i,
                    "s": random.randbytes(1024),
                    "val": value,
                }
                for i in range(current_chunk_size)
            ]
            self.ydb_client.bulk_upsert(
                table,
                column_types,
                data,
            )
            timestamp_from_ms += current_chunk_size
            rows -= current_chunk_size
            assert rows >= 0

    def write_and_check(self, table_path, count):
        ts_start = int(datetime.datetime.now().timestamp() * 1000000)
        for value in range(count):
            self.write_data(table_path, ts_start, 10, value)

        self.ydb_client.query(
            f"""
            select * from `{table_path}`
            """
        )

    @link_test_case("#15512")
    def test(self):
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/table"

        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                ts Timestamp NOT NULL,
                s String NOT NULL,
                val Uint64 NOT NULL,
                PRIMARY KEY(ts, s, val),
            )
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
            )
            """
        )

        self.write_and_check(table_path, 1)

        with pytest.raises(ydb.issues.GenericError, match=r'.*cannot allocate memory.*'):
            self.write_and_check(table_path, 600)
