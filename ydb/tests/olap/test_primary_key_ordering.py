import decimal
import json
import logging
import random
import time
import unittest
import uuid

import pytest
import ydb
from parameterized import parameterized_class

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR


logger = logging.getLogger(__name__)


class PrimaryKeyOrderMismatch(AssertionError):
    pass


def integer_values(lower, upper):
    boundaries = [-65537, -65536, -257, -256, -255, -1, 0, 1, 127, 128, 255, 256, 257, 65535, 65536]
    return sorted({lower, upper, *(value for value in boundaries if lower <= value <= upper)})


@parameterized_class([
    {"name": "with_interval_tree", "interval_tree": True},
    {"name": "without_interval_tree", "interval_tree": False},
])
class TestPrimaryKeyOrdering(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = KikimrConfigGenerator(
            nodes=1,
            column_shard_config={
                "enable_interval_tree_for_metadata_select": cls.interval_tree,
                "alter_object_enabled": True,
                "periodic_wakeup_activation_period_ms": 1000,
            },
        )
        cls.cluster = KiKiMR(config)
        cls.addClassCleanup(cls.cluster.stop)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.database = f"/{config.domain_name}"
        cls.driver = ydb.Driver(endpoint=f"grpc://{node.host}:{node.port}", database=cls.database)
        cls.addClassCleanup(cls.driver.stop)
        cls.driver.wait(timeout=60, fail_fast=True)
        cls.pool = ydb.QuerySessionPool(cls.driver)
        cls.addClassCleanup(cls.pool.stop)

    def query(self, query, parameters=None):
        return self.pool.execute_with_retries(query, parameters=parameters)

    def set_compaction(self, table, duration, count):
        features = json.dumps({"levels": [
            {"class_name": "Zero", "portions_live_duration": duration,
             "expected_blobs_size": 1572864, "portions_count_available": count},
            {"class_name": "Zero"},
        ]})
        self.query(f"""
            ALTER OBJECT `{table}` (TYPE TABLE) SET (
                ACTION=UPSERT_OPTIONS,
                `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`,
                `COMPACTION_PLANNER.FEATURES`=`{features}`
            );
        """)

    def wait_compaction(self, table):
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            stats = self.query(f"""
                SELECT Kind, COUNT(*) AS portions
                FROM `{table}/.sys/primary_index_portion_stats`
                WHERE Activity = 1 GROUP BY Kind;
            """)[0].rows
            counts = {row["Kind"]: row["portions"] for row in stats}
            if counts.get("SPLIT_COMPACTED", 0) and not counts.get("INSERTED", 0):
                return
            time.sleep(0.5)
        self.fail(f"Compaction did not finish for {table}: {stats}")

    def assert_query(self, tables, suffix, parameters, pushdown, context):
        statements = [f"SELECT id, revision FROM `{table}` {suffix};" for table in tables]
        result_sets = self.query(
            f'PRAGMA Kikimr.OptEnableOlapPushdown = "{str(pushdown).lower()}";\n'
            + "\n".join(statements), parameters,
        )
        self.assertEqual(len(result_sets), 2, context)
        row_result, column_result = [[(row["id"], row["revision"]) for row in result.rows] for result in result_sets]
        if column_result != row_result:
            raise PrimaryKeyOrderMismatch(
                f"{context}; pushdown={pushdown}; {suffix}; {parameters}; "
                f"row_result={row_result}; column_result={column_result}"
            )

    def check_reads(self, tables, sdk_type, values, composite, expected, phase):
        context = f"{tables[1]}; interval_tree={self.interval_tree}; phase={phase}"
        for table in tables:
            rows = self.query(f"SELECT id, revision FROM `{table}` ORDER BY id;")[0].rows
            self.assertEqual([(row["id"], row["revision"]) for row in rows], sorted(expected.items()), context)

        order = "prefix, k" if composite else "k"
        prefix = "prefix = 1 AND " if composite else ""
        for pushdown in (False, True):
            for direction in ("ASC", "DESC"):
                ordering = ", ".join(f"{column} {direction}" for column in order.split(", "))
                for limit in ("", " LIMIT 1", " LIMIT 3"):
                    self.assert_query(tables, f"ORDER BY {ordering}{limit}", None, pushdown, context)
            if composite:
                self.assert_query(tables, "WHERE prefix = 1 ORDER BY k", None, pushdown, context)
            for value in values:
                for operator in ("=", "<", "<=", ">", ">="):
                    self.assert_query(
                        tables, f"WHERE {prefix}k {operator} $bound ORDER BY {order}",
                        {"$bound": (value, sdk_type)}, pushdown, context,
                    )

    def check_type(self, name, values, sdk_type=None):
        sdk_type = sdk_type or getattr(ydb.PrimitiveType, "UUID" if name == "Uuid" else name)
        row_type = (ydb.StructType().add_member("k", sdk_type)
                    .add_member("prefix", ydb.PrimitiveType.Uint32)
                    .add_member("id", ydb.PrimitiveType.Uint32)
                    .add_member("revision", ydb.PrimitiveType.Uint32))
        columns = (ydb.BulkUpsertColumns().add_column("k", sdk_type)
                   .add_column("prefix", ydb.PrimitiveType.Uint32)
                   .add_column("id", ydb.PrimitiveType.Uint32)
                   .add_column("revision", ydb.PrimitiveType.Uint32))

        for composite in (False, True):
            table_name = f"{self.database}/{self._testMethodName}_{int(composite)}"
            tables = [table_name + "_row", table_name + "_column"]
            pk = "prefix, k" if composite else "k"
            for table, store in zip(tables, ("ROW", "COLUMN")):
                self.query(f"""
                    CREATE TABLE `{table}` (
                        k {name} NOT NULL, prefix Uint32 NOT NULL,
                        id Uint32 NOT NULL, revision Uint32 NOT NULL,
                        PRIMARY KEY ({pk})
                    ) WITH (STORE = {store}, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);
                """)
            self.set_compaction(tables[1], "1h", 10000)

            rows = [{"k": value, "prefix": prefix, "id": prefix * len(values) + index, "revision": 0}
                    for prefix in range(2 if composite else 1) for index, value in enumerate(values)]
            random.Random(617).shuffle(rows)
            for table in tables:
                self.driver.table_client.bulk_upsert(table, rows, columns)
            expected = {row["id"]: row["revision"] for row in rows}
            self.check_reads(tables, sdk_type, values, composite, expected, "bulk_upsert")

            for revision in (1, 2):
                updated = [dict(row, revision=revision) for row in rows if row["id"] % 2 == revision % 2]
                for table in tables:
                    self.query(f"UPSERT INTO `{table}` SELECT * FROM AS_TABLE($rows);",
                               {"$rows": (updated, ydb.ListType(row_type))})
                expected.update({row["id"]: row["revision"] for row in updated})
            self.check_reads(tables, sdk_type, values, composite, expected, "sql_upsert")

            self.set_compaction(tables[1], "1s", 2)
            self.wait_compaction(tables[1])
            self.check_reads(tables, sdk_type, values, composite, expected, "compacted")
            logger.info("PK ordering passed: %s, composite=%s, interval_tree=%s", name, composite, self.interval_tree)

    def test_bool(self):
        self.check_type("Bool", [False, True])

    def test_int8(self):
        self.check_type("Int8", integer_values(-(2 ** 7), 2 ** 7 - 1))

    def test_uint8(self):
        self.check_type("Uint8", integer_values(0, 2 ** 8 - 1))

    def test_int16(self):
        self.check_type("Int16", integer_values(-(2 ** 15), 2 ** 15 - 1))

    def test_uint16(self):
        self.check_type("Uint16", integer_values(0, 2 ** 16 - 1))

    def test_int32(self):
        self.check_type("Int32", integer_values(-(2 ** 31), 2 ** 31 - 1))

    def test_uint32(self):
        self.check_type("Uint32", integer_values(0, 2 ** 32 - 1))

    def test_int64(self):
        self.check_type("Int64", integer_values(-(2 ** 63), 2 ** 63 - 1))

    def test_uint64(self):
        self.check_type("Uint64", integer_values(0, 2 ** 64 - 1))

    def test_date(self):
        self.check_type("Date", integer_values(0, 49672))

    def test_datetime(self):
        self.check_type("Datetime", integer_values(0, 49673 * 86400 - 1))

    def test_timestamp(self):
        self.check_type("Timestamp", integer_values(0, 49673 * 86400000000 - 1))

    def test_interval(self):
        limit = 49673 * 86400000000 - 1
        self.check_type("Interval", integer_values(-limit, limit))

    def test_date32(self):
        self.check_type("Date32", integer_values(-53375809, 53375807))

    def test_datetime64(self):
        self.check_type("Datetime64", integer_values(-4611669897600, 4611669811199))

    def test_timestamp64(self):
        self.check_type("Timestamp64", integer_values(-4611669897600000000, 4611669811199999999))

    def test_interval64(self):
        limit = 9223339708799999999
        self.check_type("Interval64", integer_values(-limit, limit))

    def test_string(self):
        self.check_type("String", [b"", b"\0", b"\0a", b"a", b"a\0", b"aa", b"\x7f", b"\x80", b"\xff"])

    def test_utf8(self):
        self.check_type("Utf8", ["", "\0", "a", "a\0", "aa", "e\u0301", "é", "я", "中", "\uffff", "\U00010000", "😀"])

    def test_uuid(self):
        values = [uuid.UUID(int=0), uuid.UUID(int=(1 << 128) - 1)]
        values += [uuid.UUID(bytes_le=bytes(1 if index == byte else 0 for index in range(16))) for byte in range(16)]
        self.check_type("Uuid", values)

    def test_dynumber(self):
        self.check_type("DyNumber", [
            "-1e125", "-256", "-255", "-1", "-0.01", "0", "0.01", "1", "2", "10", "255", "256", "1e125",
        ])

    @pytest.mark.xfail(
        strict=True, raises=PrimaryKeyOrderMismatch, reason="Decimal PK still uses physical fixed_size_binary order",
    )
    def test_decimal(self):
        self.check_type("Decimal(22,9)", [decimal.Decimal(value) for value in ("-100", "-1", "0", "2", "10", "100")],
                        ydb.DecimalType(22, 9))
