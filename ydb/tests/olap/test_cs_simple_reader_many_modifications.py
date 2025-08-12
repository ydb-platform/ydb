import yatest.common
import os
import logging
import time
import random

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class Timer:
    def __init__(self, label: str):
        self.label = label
        self._t0 = None
        self._dt = None

    def __enter__(self):
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dt = time.perf_counter() - self._t0
        print(f"{self.label}: {self.dt*1000:.1f} ms", flush=True)

    def get_time_ms(self):
        return self.dt * 1000


class TestCsSimpleReaderManyModifications(object):
    test_name = "cs_simple_reader_many_modifications"

    @classmethod
    def setup_class(cls):
        cls._setup_ydb()

    @classmethod
    def teardown_class(cls):
        cls.ydb_client.stop()
        cls.cluster.stop()

    @classmethod
    def _setup_ydb(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(
            yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8")
        )
        config = KikimrConfigGenerator(
            column_shard_config={
                "compaction_enabled": False,
                "deduplication_enabled": True,
                "reader_class_name": "SIMPLE",
            }
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(
            database=f"/{config.domain_name}",
            endpoint=f"grpc://{node.host}:{node.port}",
        )
        cls.ydb_client.wait_connection()
        cls.test_dir = f"{cls.ydb_client.database}/{cls.test_name}"

    def _check_rows_num(self, table_path, expected_rows_num):
        count = self.ydb_client.query(f"SELECT COUNT(*) as cnt FROM `{table_path}`;")[
            0
        ].rows[0]["cnt"]
        assert count == expected_rows_num

    def test_many_modifications(self):
        table_path = f"{self.test_dir}/test_many_modifications"
        with Timer("drop table"):
            self.ydb_client.query(f"DROP TABLE IF EXISTS `{table_path}`")
        with Timer("create table"):
            self.ydb_client.query(
                f"""
                CREATE TABLE `{table_path}` (
                    id Int32 NOT NULL,
                    value Int64,
                    PRIMARY KEY(id),
                )
                WITH (
                    STORE = COLUMN
                )
                """
            )

        rows_num = 10
        mods_num = 5
        # Generate a list of primary keys
        pks = list(range(rows_num))

        # Generate a series of modifications for each key
        modifications = {}
        for pk in pks:
            modifications[pk] = []
            for _ in range(mods_num):
                modifications[pk].append(random.randint(0, 1000000))

        # Insert the initial rows
        with Timer("initial inserts"):
            for pk in pks:
                initial_value = modifications[pk][0]
                self.ydb_client.query(
                    f"INSERT INTO `{table_path}` (id, value) VALUES ({pk}, {initial_value});"
                )

        self._check_rows_num(table_path, rows_num)

        # Apply the subsequent modifications using UPDATE
        updates_num = 0
        updates_timer = Timer("updates")
        with updates_timer:
            for pk in pks:
                for value in modifications[pk][1:]:
                    self.ydb_client.query(
                        f"UPDATE `{table_path}` SET value = {value} WHERE id = {pk};"
                    )
                    updates_num += 1
        print(
            f"updates_num: {updates_num}, avg_time: {updates_timer.get_time_ms() / updates_num} ms"
        )

        # Verify the final state
        self._check_rows_num(table_path, rows_num)

        expected_data = {}
        for pk in pks:
            expected_data[pk] = modifications[pk][-1]

        # Select all rows in bulk
        with Timer("bulk select"):
            result_sets = self.ydb_client.query(
                f"SELECT id, value FROM `{table_path}`;"
            )
            rows = [row for result_set in result_sets for row in result_set.rows]

        assert len(rows) == rows_num
        actual_data = {row["id"]: row["value"] for row in rows}

        assert actual_data == expected_data
