import yatest.common
import os
import logging
import time
import random
from enum import Enum
import pytest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
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
        self._dt = None

    def __enter__(self):
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._dt = time.perf_counter() - self._t0
        print(f"{self.label}: {self._dt*1000:.1f} ms", flush=True)

    def get_time_ms(self):
        return self._dt * 1000


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

    def _do_update(self, table_path, pks, mods):
        """Apply modifications using individual UPDATE statements."""
        updates_num = 0
        for pk in pks:
            for value in mods[pk]:
                update_query = f"""
                UPDATE `{table_path}`
                SET value = {value}
                WHERE id = {pk};
                """
                self.ydb_client.query(update_query)
                updates_num += 1
        return updates_num

    def _do_upsert(self, table_path, pks, mods):
        """Apply modifications using individual UPSERT statements."""
        updates_num = 0
        for pk in pks:
            for value in mods[pk]:
                upsert_query = f"""
                UPSERT INTO `{table_path}` (id, value)
                VALUES ({pk}, {value});
                """
                self.ydb_client.query(upsert_query)
                updates_num += 1
        return updates_num

    def _do_bulk_upsert(self, table_path, pks, mods):
        """Apply modifications using batch UPSERT statements."""
        updates_num = 0
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
            updates_num += 1
        return updates_num

    def _test_many_updates_impl(self, rows_num, operation_sequence):
        """Test a sequence of different modification operations on the same table."""
        table_path = f"{self.test_dir}/test_many_modifications_sequence"

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

        # Generate a list of primary keys
        pks = list(range(rows_num))

        # Generate initial values
        inits = [random.randint(0, 1000000) for _ in range(rows_num)]

        # Insert the initial rows
        with Timer("initial inserts"):
            for pk in pks:
                initial_value = inits[pk]
                self.ydb_client.query(
                    f"INSERT INTO `{table_path}` (id, value) VALUES ({pk}, {initial_value});"
                )

        with Timer("select count(*) after initial inserts"):
            self._check_rows_num(table_path, rows_num)

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
            operation_timer = Timer(f"{operation_name}s (step {total_operations + 1})")
            with operation_timer:
                if mod_type == ModType.UPDATE:
                    updates_num = self._do_update(table_path, pks, mods)
                elif mod_type == ModType.UPSERT:
                    updates_num = self._do_upsert(table_path, pks, mods)
                elif mod_type == ModType.BULK_UPSERT:
                    updates_num = self._do_bulk_upsert(table_path, pks, mods)
                else:
                    raise ValueError(f"Unsupported modification type: {mod_type}")

            print(
                f"{operation_name}s num: {updates_num}, avg_time: {operation_timer.get_time_ms() / updates_num:.2f} ms"
            )
            total_operations += 1

        # Verify the final state
        with Timer("select count(*) after all modifications"):
            self._check_rows_num(table_path, rows_num)

        expected_data = {pk: expected_values[pk] for pk in pks}

        with Timer("select all"):
            result_sets = self.ydb_client.query(
                f"SELECT id, value FROM `{table_path}`;"
            )

        rows = [row for result_set in result_sets for row in result_set.rows]
        assert len(rows) == rows_num
        actual_data = {row["id"]: row["value"] for row in rows}
        assert actual_data == expected_data

    @pytest.mark.parametrize(
        "rows_num,operation_sequence",
        [
            # Single operations
            (10, [{"mod_type": ModType.UPDATE, "mods_num": 2}]),
            (10, [{"mod_type": ModType.UPSERT, "mods_num": 100}]),
            (30, [{"mod_type": ModType.BULK_UPSERT, "mods_num": 500}]),

            # Sequential combinations
            (15, [
                {"mod_type": ModType.UPDATE, "mods_num": 3},
                {"mod_type": ModType.BULK_UPSERT, "mods_num": 10}
            ]),
            (20, [
                {"mod_type": ModType.UPDATE, "mods_num": 3},
                {"mod_type": ModType.UPSERT, "mods_num": 7},
                {"mod_type": ModType.BULK_UPSERT, "mods_num": 15}
            ]),
            (10, [
                {"mod_type": ModType.BULK_UPSERT, "mods_num": 5},
                {"mod_type": ModType.UPDATE, "mods_num": 2},
                {"mod_type": ModType.UPSERT, "mods_num": 8}
            ]),
        ],
        ids=[
            "update",
            "upsert",
            "bulk_upsert",
            "update_bulk_upsert",
            "update_upsert_bulk_upsert",
            "bulk_upsert_update_upsert"
        ]
    )
    def test_many_updates(self, rows_num, operation_sequence):
        """Test sequences of different database modification operations."""
        self._test_many_updates_impl(
            rows_num=rows_num, operation_sequence=operation_sequence
        )

    def test_many_updates_random_sequence(self):
        """Test random sequence of different datebase modification operations."""
        # every update takes about 3 seconds, delete this flag when updates become fast enough
        update_are_too_slow = True
        rows_num = 10
        op_count = 10
        operation_sequence = []
        for _ in range(op_count):
            op_type = random.choice(list(ModType))
            max_mods_num = 50
            if op_type == ModType.UPDATE and update_are_too_slow:
                max_mods_num = 2
            op_num = random.randint(1, max_mods_num)
            operation_sequence.append({"mod_type": op_type, "mods_num": op_num})

        # Print the generated operation sequence for debugging
        print("Generated operation sequence:")
        for i, op in enumerate(operation_sequence):
            print(f"  {i}. {op["mod_type"].name}: {op["mods_num"]} operations")

        self._test_many_updates_impl(
            rows_num=rows_num, operation_sequence=operation_sequence
        )
