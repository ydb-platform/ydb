import yatest.common
import os
import logging
import time
import random
from enum import Enum
import pytest
import statistics
from textwrap import dedent

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
# from ydb.tests.library.harness.util import LogLevels
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class ModType(Enum):
    """Enum to define different types of database modification operations."""

    UPDATE = "update"
    UPSERT = "upsert"
    BULK_UPSERT = "bulk_upsert"


class Timer:
    def __init__(self, label: str):
        self.label = label
        self._t0 = None
        self._dts = []
        self._to_ms = 1000

    def __enter__(self):
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if len(self._dts) == 0:
            self.one_more()
        self._dts.sort()

    def one_more(self):
        new_t0 = time.perf_counter()
        dt = new_t0 - self._t0
        self._dts.append(dt)
        self._t0 = new_t0

    def report(self):
        if len(self._dts) == 1:
            return f"{self.label}: {self._dts[0] * self._to_ms:.1f} ms"

        return dedent(
            f"""
        {self.label}:
            "runs": {len(self._dts)}
            "avg_ms": {self._get_mean():.1f}
            "min_ms": {self._get_d(0):.1f}
            "median_ms": {self._get_d(len(self._dts) // 2):.1f}
            "max_ms": {self._get_d(-1):.1f}
            "stdev_ms": {self._get_stdev():.1f}
        """
        ).strip()

    def _get_d(self, idx):
        return self._dts[idx] * self._to_ms

    def _get_mean(self):
        return statistics.mean(self._dts) * self._to_ms

    def _get_stdev(self):
        return statistics.stdev(self._dts) * self._to_ms


class TestCSManyUpdates(object):
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
            },
            # Uncomment this to see more logs
            # additional_log_configs={
            #     "TX_COLUMNSHARD_TX": LogLevels.TRACE,
            #     "TX_COLUMNSHARD": LogLevels.TRACE,
            # },
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
        result_sets = self.ydb_client.query(
            f"SELECT COUNT(*) as cnt FROM `{table_path}`;"
        )
        assert len(result_sets) == 1
        count = result_sets[0].rows[0]["cnt"]
        assert count == expected_rows_num

    def _do_update(self, table_path, pks, mods, timer):
        """Apply modifications using individual UPDATE statements."""
        for pk in pks:
            for value in mods[pk]:
                update_query = f"""
                UPDATE `{table_path}`
                SET value = {value}
                WHERE id = {pk};
                """
                self.ydb_client.query(update_query)
                timer.one_more()

    def _do_upsert(self, table_path, pks, mods, timer):
        """Apply modifications using individual UPSERT statements."""
        for pk in pks:
            for value in mods[pk]:
                upsert_query = f"""
                UPSERT INTO `{table_path}` (id, value)
                VALUES ({pk}, {value});
                """
                self.ydb_client.query(upsert_query)
                timer.one_more()

    def _do_bulk_upsert(self, table_path, pks, mods, timer):
        """Apply modifications using batch UPSERT statements."""
        mods_num = len(mods[0])
        for mod_idx in range(mods_num):
            # Collect all updates for this round
            update_values = []
            for pk in pks:
                value = mods[pk][mod_idx]
                update_values.append(f"({pk}, {value})")

            # Execute single batch UPSERT for all PKs in this round
            values_clause = ", ".join(update_values)
            batch_upsert_query = f"""
            UPSERT INTO `{table_path}` (id, value)
            VALUES {values_clause};
            """
            self.ydb_client.query(batch_upsert_query)
            timer.one_more()

    def _test_many_updates_impl(self, rows_num, operation_sequence):
        """Test a sequence of different modification operations on the same table."""
        table_path = f"{self.test_dir}/test_many_modifications_sequence"

        timer = Timer("drop table")
        with timer:
            self.ydb_client.query(f"DROP TABLE IF EXISTS `{table_path}`")
        print(timer.report())

        timer = Timer("create table")
        with timer:
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
        print(timer.report())

        # Generate a list of primary keys
        pks = list(range(rows_num))

        # Generate initial values
        inits = [random.randint(0, 1000000) for _ in range(rows_num)]

        # Insert the initial rows
        timer = Timer("initial inserts")
        with timer:
            for pk in pks:
                initial_value = inits[pk]
                self.ydb_client.query(
                    f"INSERT INTO `{table_path}` (id, value) VALUES ({pk}, {initial_value});"
                )
                timer.one_more()
        print(timer.report())

        timer = Timer("select count(*) after initial inserts")
        with timer:
            self._check_rows_num(table_path, rows_num)
        print(timer.report())

        # Keep track of expected final values
        expected_values = inits[:]
        total_operations = 0

        # Execute each operation in the sequence
        for operation in operation_sequence:
            mod_type = operation["mod_type"]
            mods_num = operation["mods_num"]
            operation_name = mod_type.value

            # Generate modifications for this operation
            mods = [
                [random.randint(0, 1000000) for _ in range(mods_num)]
                for _ in range(rows_num)
            ]

            # Update expected values with the last modification from this operation
            for pk in pks:
                expected_values[pk] = mods[pk][-1]

            # Execute the operation
            timer = Timer(f"{operation_name}s (step {total_operations})")
            with timer:
                if mod_type == ModType.UPDATE:
                    self._do_update(table_path, pks, mods, timer)
                elif mod_type == ModType.UPSERT:
                    self._do_upsert(table_path, pks, mods, timer)
                elif mod_type == ModType.BULK_UPSERT:
                    self._do_bulk_upsert(table_path, pks, mods, timer)
                else:
                    raise ValueError(f"Unsupported modification type: {mod_type}")

            print(timer.report())
            total_operations += 1

        # Verify the final state
        timer = Timer("select count(*) after all modifications")
        with timer:
            self._check_rows_num(table_path, rows_num)
        print(timer.report())

        expected_data = {pk: expected_values[pk] for pk in pks}

        timer = Timer("select all")
        with timer:
            result_sets = self.ydb_client.query(
                f"SELECT id, value FROM `{table_path}`;"
            )
        print(timer.report())

        rows = [row for result_set in result_sets for row in result_set.rows]
        assert len(rows) == rows_num
        actual_data = {row["id"]: row["value"] for row in rows}
        assert actual_data == expected_data

    @pytest.mark.parametrize(
        "rows_num,operation_sequence",
        [
            # On practice, 10 rows * 100 updates runs for 100 seconds approximately.
            # So, there is no point in setting larger numbers here.
            # BULK_UPSERT updates all the rows at once, so a bit different math for it. 10 * 500 works good enough.
            # Single operations
            (10, [{"mod_type": ModType.UPDATE, "mods_num": 100}]),
            (10, [{"mod_type": ModType.UPSERT, "mods_num": 100}]),
            (10, [{"mod_type": ModType.BULK_UPSERT, "mods_num": 500}]),
            # Sequential combinations
            (
                15,
                [
                    {"mod_type": ModType.UPDATE, "mods_num": 10},
                    {"mod_type": ModType.BULK_UPSERT, "mods_num": 100},
                ],
            ),
            (
                20,
                [
                    {"mod_type": ModType.UPDATE, "mods_num": 10},
                    {"mod_type": ModType.UPSERT, "mods_num": 10},
                    {"mod_type": ModType.BULK_UPSERT, "mods_num": 100},
                ],
            ),
            (
                10,
                [
                    {"mod_type": ModType.BULK_UPSERT, "mods_num": 100},
                    {"mod_type": ModType.UPDATE, "mods_num": 20},
                    {"mod_type": ModType.UPSERT, "mods_num": 20},
                ],
            ),
        ],
        ids=[
            "update",
            "upsert",
            "bulk_upsert",
            "update_bulk_upsert",
            "update_upsert_bulk_upsert",
            "bulk_upsert_update_upsert",
        ],
    )
    def test_many_updates(self, rows_num, operation_sequence):
        """Test sequences of different database modification operations."""
        self._test_many_updates_impl(
            rows_num=rows_num, operation_sequence=operation_sequence
        )

    def test_many_updates_random_sequence(self):
        """Test random sequence of different datebase modification operations."""
        rows_num = 10
        op_count = 10
        operation_sequence = []
        for _ in range(op_count):
            op_type = random.choice(list(ModType))
            max_mods_num = 20
            op_num = random.randint(1, max_mods_num)
            operation_sequence.append({"mod_type": op_type, "mods_num": op_num})

        # Print the generated operation sequence for debugging
        print("Generated operation sequence:")
        for i, op in enumerate(operation_sequence):
            print(f"  {i}. {op['mod_type'].name}: {op['mods_num']} operations")

        self._test_many_updates_impl(
            rows_num=rows_num, operation_sequence=operation_sequence
        )
