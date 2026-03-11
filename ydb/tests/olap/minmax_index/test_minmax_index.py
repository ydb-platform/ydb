# -*- coding: utf-8 -*-
import os
import time
import logging
from datetime import date, datetime
import yatest.common
import ydb

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.sql.lib.test_base import TestBase

logger = logging.getLogger(__name__)


def _format_sql_value(value, type_name):
    """Format a Python value as a YQL literal for the given type."""
    if value is None:
        return "NULL"
    if type_name in ("String", "Utf8"):
        return f"'{value}'"
    if type_name == "Bool":
        return "True" if value else "False"
    if type_name in ("Date", "Date32"):
        if isinstance(value, (date, datetime)):
            value = value.strftime("%Y-%m-%d")
        return f"CAST('{value}' AS {type_name})"
    if type_name in ("Datetime", "Datetime64"):
        if isinstance(value, datetime):
            value = value.strftime("%Y-%m-%dT%H:%M:%SZ")
        return f"CAST('{value}' AS {type_name})"
    # Numeric types: Int*, Uint*, Float, Double, Timestamp, Timestamp64, Interval, Interval64
    return f"CAST({value} AS {type_name})"


def _wait_compaction(pool, table_full_path, timeout=120):
    """Wait until the tiling compaction optimizer has no pending work."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = pool.execute_with_retries(f"""
            SELECT COUNT(*) AS cnt
            FROM `{table_full_path}/.sys/primary_index_optimizer_stats`
            WHERE CAST(JSON_VALUE(CAST(Details AS JsonDocument), "$.weight") AS Uint64) > 0
        """)
        if result[0].rows[0]["cnt"] == 0:
            return
        time.sleep(1)
    raise TimeoutError(f"Compaction of {table_full_path} did not complete within {timeout}s")


class TestYdbMinMaxIndex(TestBase):

    ALL_TYPES = {
        "Int8": lambda i: i % 100,
        "Int16": lambda i: i,
        "Int32": lambda i: i,
        "Int64": lambda i: i,
        "Uint8": lambda i: i % 256,
        "Uint16": lambda i: i,
        "Uint32": lambda i: i,
        "Uint64": lambda i: i,
        "Bool": lambda i: i % 2 == 0,
        "Float": lambda i: float(i) + 0.1,
        "Double": lambda i: float(i) + 0.2,
        "String": lambda i: f"str_{i}",
        "Utf8": lambda i: f"utf8_{i}",
        "Date": lambda i: date(2000 + (i % 105), 1, 1),
        "Datetime": lambda i: datetime(2000 + (i % 105), 1, 1, 12, 0, 0),
        # "Timestamp": lambda i: 1696200000000000 + i * 100000, #see gh35699
    }

    @classmethod
    def setup_class(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY", "ydb/apps/ydbd/ydbd"))
        logger.error(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))

        cls.ydb_cli_path = yatest.common.build_path("ydb/apps/ydb/ydb")

        cls.database = "/Root"
        cls.cluster = KiKiMR(KikimrConfigGenerator(
            erasure=cls.get_cluster_configuration(),
            column_shard_config={
                'alter_object_enabled': True,
                'lag_for_compaction_before_tierings_ms': 0,
                'compaction_actualization_lag_ms': 0,
                'optimizer_freshness_check_duration_ms': 0,
                'small_portion_detect_size_limit': 0,
            },
        ))

        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database=cls.get_database(),
                endpoint=cls.get_endpoint()
            )
        )
        print(cls.get_endpoint())

        cls.driver.wait()
        cls.pool = ydb.QuerySessionPool(cls.driver)

    def test_minmax_index_smoke(self):
        """
        Test adding columns and verifying that minmax index is built correctly
        """

        self.query("DROP TABLE IF EXISTS `minmax_index_all_types`;")
        self.query("""
            CREATE TABLE `minmax_index_all_types` (
                C0 Uint64 NOT NULL,
                PRIMARY KEY (C0)
            ) WITH (STORE = COLUMN);
        """)
        # Enable fast tiling compaction and wait for it to finish
        self.query(f"""
            ALTER OBJECT `{self.database}/minmax_index_all_types` (TYPE TABLE) SET (
                ACTION=UPSERT_OPTIONS,
                `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`,
                `COMPACTION_PLANNER.FEATURES`=`{{"levels": [
                    {{"class_name": "Zero", "portions_live_duration": "5s", "expected_blobs_size": 1572864, "portions_count_available": 2}},
                    {{"class_name": "Zero"}}
                ]}}`
            );
        """)

        type_names = list(self.ALL_TYPES.keys())

        # Add columns one by one, each of different type
        for i, (type_name, type_func) in enumerate(self.ALL_TYPES.items()):
            col_name = f"C{i + 1}"
            self.query(f"""
                ALTER TABLE `minmax_index_all_types`
                ADD COLUMN {col_name} {type_name};
            """)
            # Add minmax index on the column
            self.query(f"""
                ALTER OBJECT `{self.database}/minmax_index_all_types` (TYPE TABLE) SET (
                    ACTION=UPSERT_INDEX, NAME=idx_{col_name}_minmax, TYPE=MINMAX,
                    FEATURES=`{{"column_name": "{col_name}"}}`
                );
            """)

        # Insert rows: every 10th row has all nullable columns set to NULL
        values = []
        for row_idx in range(5000):
            row = [row_idx]  # C0 is Uint64 PK NOT NULL
            for type_name, type_func in self.ALL_TYPES.items():
                row.append(None if row_idx % 10 == 0 else type_func(row_idx))
            values.append(tuple(row))

        columns = ["C0"] + [f"C{i + 1}" for i in range(len(self.ALL_TYPES))]
        # Chunked UPSERT for bulk insert
        chunk_size = 1000
        for chunk_start in range(0, len(values), chunk_size):
            chunk = values[chunk_start:chunk_start+chunk_size]
            values_sql = ", ".join([
                "(" + ", ".join(
                    str(val) if i == 0 else _format_sql_value(val, type_names[i - 1])
                    for i, val in enumerate(row)
                ) + ")"
                for row in chunk
            ])
            self.query(f"""
                UPSERT INTO `minmax_index_all_types` ({", ".join(columns)}) VALUES {values_sql};
            """)

        _wait_compaction(self.pool, f"{self.database}/minmax_index_all_types")

        # Perform a separate SELECT for each column with a simple equality predicate
        for i, (type_name, type_func) in enumerate(self.ALL_TYPES.items()):
            col_name = f"C{i + 1}"
            if type_name == "String":
                val = "str_42"
            elif type_name == "Utf8":
                val = "utf8_42"
            elif type_name == "Bool":
                val = i % 2 == 0
            else:
                val = type_func(i)

            self.query(f"""
                SELECT C0 FROM `minmax_index_all_types` WHERE {col_name} = {_format_sql_value(val, type_name)} ORDER BY C0;
            """)
