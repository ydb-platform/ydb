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
        return f"CAST({value} AS {type_name})"  # int: days since Unix epoch
    if type_name in ("Datetime", "Datetime64"):
        if isinstance(value, datetime):
            value = value.strftime("%Y-%m-%dT%H:%M:%SZ")
            return f"CAST('{value}' AS {type_name})"
        return f"CAST({value} AS {type_name})"  # int: seconds since Unix epoch
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
        "Float": lambda i: float(i),  # exact integer float, no precision issues
        "Double": lambda i: float(i) + 0.5,  # 0.5 is exactly representable in binary
        "String": lambda i: f"str_{i}",
        "Utf8": lambda i: f"utf8_{i}",
        "Date": lambda i: 10957 + i,  # days since Unix epoch (10957 = 2000-01-01)
        "Datetime": lambda i: 1000000000 + i * 60,  # seconds since Unix epoch
        "Timestamp": lambda i: 1696200000000000 + i * 100000,
    }

    # YQL expressions producing the same values as ALL_TYPES lambdas, used in the ListMap UPSERT.
    YQL_COL_EXPRS = {
        "Int8":      "CAST($x % 100 AS Int8)",
        "Int16":     "CAST($x AS Int16)",
        "Int32":     "CAST($x AS Int32)",
        "Int64":     "CAST($x AS Int64)",
        "Uint8":     "CAST($x % 256 AS Uint8)",
        "Uint16":    "CAST($x AS Uint16)",
        "Uint32":    "CAST($x AS Uint32)",
        "Uint64":    "CAST($x AS Uint64)",
        "Bool":      "($x % 2 == 0)",
        "Float":     "CAST($x AS Float)",
        "Double":    "CAST($x AS Double) + 0.5",
        "String":    '"str_" || CAST($x AS String)',
        "Utf8":      'Utf8("utf8_") || CAST($x AS Utf8)',
        "Date":      "CAST(10957 + $x AS Date)",
        "Datetime":  "CAST(1000000000 + $x * 60 AS Datetime)",
        "Timestamp": "CAST(1696200000000000 + $x * 100000 AS Timestamp)",
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
            disabled_feature_flags=[
                'enable_local_index_as_scheme_object',
            ],
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

        # Insert 5000 rows via SQL: every 10th row has all nullable columns set to NULL.
        # Data is generated entirely in SQL using ListMap + AS_TABLE — no Python value serialization.
        # UNWRAP is required: CAST(Int64 AS Uint64) yields Optional<Uint64> in YQL,
        # but C0 is NOT NULL — UNWRAP asserts non-null at runtime.
        struct_fields = "UNWRAP(CAST($x AS Uint64)) AS C0"
        for i, (type_name, _) in enumerate(self.ALL_TYPES.items()):
            expr = self.YQL_COL_EXPRS[type_name]
            struct_fields += f",\n            IF($x % 10 == 0, NULL, {expr}) AS C{i + 1}"

        self.query(f"""
            $data = ListMap(ListFromRange(0, 5000), ($x) -> {{
                RETURN AsStruct(
                    {struct_fields}
                );
            }});
            UPSERT INTO `minmax_index_all_types` SELECT * FROM AS_TABLE($data);
        """)

        _wait_compaction(self.pool, f"{self.database}/minmax_index_all_types")

        # Perform a separate SELECT for each column with a simple equality predicate
        # and verify that the result exactly matches what was inserted (index must not affect correctness)
        for i, (type_name, type_func) in enumerate(self.ALL_TYPES.items()):
            col_name = f"C{i + 1}"
            if type_name == "String":
                target_val = "str_42"
            elif type_name == "Utf8":
                target_val = "utf8_42"
            elif type_name == "Bool":
                target_val = i % 2 == 0
            else:
                target_val = type_func(i)

            # Compute expected C0 values using the same formula as the SQL generator above.
            # Rows where row_idx % 10 == 0 are NULL and never match.
            expected_c0 = sorted([
                row_idx for row_idx in range(5000)
                if row_idx % 10 != 0 and type_func(row_idx) == target_val
            ])

            rows = self.query(f"""
                SELECT C0 FROM `minmax_index_all_types` WHERE {col_name} = {_format_sql_value(target_val, type_name)} ORDER BY C0;
            """)
            actual_c0 = [row["C0"] for row in rows]

            assert actual_c0 == expected_c0, (
                f"Column {col_name} ({type_name}): "
                f"expected {len(expected_c0)} rows {expected_c0[:10]}, "
                f"got {len(actual_c0)} rows {actual_c0[:10]}"
            )
