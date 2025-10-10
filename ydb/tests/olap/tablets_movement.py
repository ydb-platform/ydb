import datetime
import logging
import os
import pytest
import requests
import time
import yatest.common
import ydb

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient
from ydb.tests.olap.common.thread_helper import TestThread, TestThreads

logger = logging.getLogger(__name__)


class TestTabletsMovement(object):
    test_name = "tablets_movement"

    @classmethod
    def setup_class(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            column_shard_config={
                "deduplication_enabled": True,
                "reader_class_name": "SIMPLE",
            }
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        cls.mon_url = f"http://{node.host}:{node.mon_port}"
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
                    "s": bytes(i) * 100,
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

    def list_shards(self, table_path):
        response = requests.get(
            self.mon_url
            + f"/viewer/json/describe?database={self.ydb_client.database}&path={table_path}&enums=true&partition_stats=true&subs=0"
        )
        response.raise_for_status()
        path_description = response.json()["PathDescription"]
        if "ColumnTableDescription" in path_description:
            return path_description["ColumnTableDescription"]["Sharding"]["ColumnShards"]
        return [item['DatashardId'] for item in path_description["TablePartitions"]]

    def kill_tablet(self, tablet_id):
        response = requests.get(self.mon_url + f"/tablets?RestartTabletID={tablet_id}")
        response.raise_for_status()

    def _loop_kill_tablets(self, table_path):
        start_time = time.time()
        while time.time() - start_time < 60:
            shards = self.list_shards(table_path)
            for shard in shards:
                self.kill_tablet(shard)
            time.sleep(2)

    @pytest.mark.parametrize("store", ["ROW", "COLUMN"])
    def test(self, store):
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/table"
        self.ydb_client.query(f"DROP TABLE IF EXISTS `{table_path}`")
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                ts Timestamp NOT NULL,
                s String NOT NULL,
                val Uint64 NOT NULL,
                PRIMARY KEY(ts, s, val),
            )
            WITH (
                STORE = {store},
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT=64,
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT=64
            )
            """
        )

        ts_start = int(datetime.datetime.fromisoformat('2025-08-18').timestamp() * 1000000)
        self.write_data(table_path, ts_start, 10000, 1)

        backgroundThreads: TestThreads = TestThreads()
        backgroundThreads.append(TestThread(target=self._loop_kill_tablets, args=[table_path]))
        backgroundThreads.start_all()

        start_time = time.time()
        iteration = 0
        while time.time() - start_time < 60:
            result_sets = self.ydb_client.query(
                f"""
                    select
                        String::Hex(Sum(Digest::MurMurHash32(Pickle(TableRow())))) AS data_hash,
                        COUNT(*) AS rows_count
                    from `{table_path}`
                """
            )
            assert result_sets[0].rows[0]['rows_count'] == 10000, iteration
            assert result_sets[0].rows[0]['data_hash'] == b'0x00001399F54C3FE9', iteration
            iteration += 1

        backgroundThreads.join_all()
