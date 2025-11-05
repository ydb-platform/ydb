import datetime
import logging
import os
import yatest.common
import ydb
import pytest

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class TestDataReadCorrectness(object):
    test_name = "data_read_correctness"
    rows_count = 5

    @classmethod
    def setup_class(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            column_shard_config={
                "compaction_enabled": False,
                "deduplication_enabled": False,
                "reader_class_name": "SIMPLE",
            }
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        cls.ydb_client.wait_connection()

    def write_data(self, table: str):
        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("id", ydb.PrimitiveType.Uint64)
        column_types.add_column("value", ydb.PrimitiveType.Uint64)

        data = [{"id": j, "value": j * 10} for j in range(self.rows_count)]

        self.ydb_client.bulk_upsert(
            table,
            column_types,
            data,
        )

    @pytest.mark.parametrize(
        "table_name",
        [
            "table",
            "primary_index_stats"
        ]
    )

    def test(self, table_name):
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/{table_name}"
        self.ydb_client.query(f"DROP TABLE IF EXISTS `{table_path}`")

        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                value Uint64,
                PRIMARY KEY(id),
            )
            WITH (
                STORE = COLUMN,
                PARTITION_COUNT = 1
            )
            """
        )

        self.write_data(table_path)

        result_sets = self.ydb_client.query(
            f"""
            select id, value from `{table_path}` where id = 3
            """
        )

        assert len(result_sets[0].rows) == 1

        assert result_sets[0].rows[0]['id'] == 3
        assert result_sets[0].rows[0]['value'] == 30

        result_sets = self.ydb_client.query(
            f"""
            select id, value from `{table_path}`
            """
        )

        assert len(result_sets[0].rows) == self.rows_count

        keys = [row['id'] for result_set in result_sets for row in result_set.rows]
        values = [row['value'] for result_set in result_sets for row in result_set.rows]

        assert keys == [i for i in range(self.rows_count)], keys
        assert values == [i * 10 for i in range(self.rows_count)], values




class TestDataReadPerformanceNoIntersections(object):
    test_name = "data_read_performance_no_intersections"
    rows_count = 10000

    @classmethod
    def setup_class(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            column_shard_config={
                "compaction_enabled": False,
            }
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(database=f"/{config.domain_name}", endpoint=f"grpc://{node.host}:{node.port}")
        cls.ydb_client.wait_connection()

    def write_data(self, table: str):
        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("id", ydb.PrimitiveType.Uint64)
        column_types.add_column("value", ydb.PrimitiveType.Uint64)

        for i in range(self.rows_count):
            data = [{"id": i, "value": i}]

            self.ydb_client.bulk_upsert(
                table,
                column_types,
                data,
            )

    def test(self):
        test_dir = f"{self.ydb_client.database}/{self.test_name}"
        table_path = f"{test_dir}/table"
        self.ydb_client.query(f"DROP TABLE IF EXISTS `{table_path}`")

        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                value Uint64,
                PRIMARY KEY(id),
            )
            WITH (
                STORE = COLUMN,
                PARTITION_COUNT = 10
            )
            """
        )

        self.write_data(table_path)

        t = datetime.datetime.now()
        for _ in range(100):
            result_sets = self.ydb_client.query(
                f"""
                select count(*) as `count` from `{table_path}`
                """
            )

            assert len(result_sets) == 1, f"Expected 1 result set, got {len(result_sets)}"
            assert len(result_sets[0].rows) == 1, f"Expected 1 row in result set, got {len(result_sets[0].rows)}"

            count_value = result_sets[0].rows[0]['count']
            assert count_value == self.rows_count, f"Expected COUNT(*) to return {self.rows_count}, but got {count_value}"

            assert datetime.datetime.now() - t < datetime.timedelta(seconds=180)
