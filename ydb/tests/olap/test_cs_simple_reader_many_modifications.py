import yatest.common
import os
import logging
import time
import random
from enum import Enum

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

    def _test_many_modifications(self, rows_num, mods_num, mod_type: ModType):
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

        # Generate a list of primary keys
        pks = list(range(rows_num))

        # Generate a series of initial values and modifications for each key
        inits = [random.randint(0, 1000000) for _ in range(rows_num)]
        mods = [
            [random.randint(0, 1000000) for _ in range(mods_num)]
            for _ in range(rows_num)
        ]

        # Insert the initial rows
        with Timer("initial inserts"):
            for pk in pks:
                initial_value = inits[pk]
                self.ydb_client.query(
                    f"INSERT INTO `{table_path}` (id, value) VALUES ({pk}, {initial_value});"
                )

        with Timer("select count(*) after initial inserts"):
            self._check_rows_num(table_path, rows_num)

        # Apply the subsequent modifications based on modification type
        operation_name = mod_type.value
        updates_timer = Timer(f"{operation_name}s")
        with updates_timer:
            if mod_type == ModType.UPDATE:
                updates_num = self._do_update(table_path, pks, mods)
            elif mod_type == ModType.UPSERT:
                updates_num = self._do_upsert(table_path, pks, mods)
            elif mod_type == ModType.BULK_UPSERT:
                updates_num = self._do_bulk_upsert(table_path, pks, mods)
            else:
                raise ValueError(f"Unsupported modification type: {mod_type}")

        print(
            f"{operation_name}s num: {updates_num}, avg_time: {updates_timer.get_time_ms() / updates_num} ms"
        )

        # Verify the final state
        with Timer("select count(*) after modifications"):
            self._check_rows_num(table_path, rows_num)

        # it is easier to compare with a dict in this case
        expected_data = {pk: mods[pk][-1] for pk in pks}

        # Select all rows in bulk
        with Timer("select all"):
            result_sets = self.ydb_client.query(
                f"SELECT id, value FROM `{table_path}`;"
            )

        # flatten the result sets, len(result_sets) may be greater than 1 (and usually is)
        rows = [row for result_set in result_sets for row in result_set.rows]

        assert len(rows) == rows_num
        actual_data = {row["id"]: row["value"] for row in rows}

        assert actual_data == expected_data

    def test_many_modifications_with_update(self):
        """Test using individual UPDATE statements."""
        self._test_many_modifications(rows_num=10, mods_num=2, mod_type=ModType.UPDATE)

    def test_many_modifications_with_upsert(self):
        """Test using individual UPSERT statements."""
        self._test_many_modifications(
            rows_num=10, mods_num=100, mod_type=ModType.UPSERT
        )

    def test_many_modifications_with_bulk_upsert(self):
        """Test using batch UPSERT statements."""
        self._test_many_modifications(
            rows_num=30, mods_num=500, mod_type=ModType.BULK_UPSERT
        )
