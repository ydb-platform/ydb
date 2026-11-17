import contextlib
import io
import json
import math
import struct
import subprocess
import threading
import time
import unittest
import uuid
from unittest import mock

import yatest.common

from ydb.core.protos import msgbus_pb2
from ydb.public.api.protos.draft import ydb_tablet_pb2
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tools.validate_indexes import validate_indexes as validator


class TestValidateIndexes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(nodes=1, extra_feature_flags=["enable_table_pg_types"]))
        cls.addClassCleanup(cls.cluster.stop)
        cls.cluster.start()
        cls.endpoint = f"grpc://{cls.cluster.nodes[1].host}:{cls.cluster.nodes[1].port}"
        cls.driver = ydb.Driver(endpoint=cls.endpoint, database="/Root")
        cls.addClassCleanup(cls.driver.stop)
        cls.driver.wait(timeout=30, fail_fast=True)
        cls.pool = ydb.SessionPool(cls.driver)
        cls.addClassCleanup(cls.pool.stop)

    def _create(self, kind="SYNC", rows=True):
        self.table = f"/Root/validate_{uuid.uuid4().hex}"
        self.index = self.table + "/by_value/indexImplTable"
        self.pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
            CREATE TABLE `{self.table}` (
                tenant Utf8, id Uint64, value Utf8, payload Utf8,
                PRIMARY KEY (tenant, id),
                INDEX by_value GLOBAL {kind} ON (value, tenant) COVER (payload)
            );
        """))
        if rows:
            self._execute(f"""
                UPSERT INTO `{self.table}` (tenant, id, value, payload) VALUES
                    ('a', 1u, 'first', 'one'),
                    ('a', 2u, NULL, 'two'),
                    (NULL, 3u, 'third', NULL);
            """)
            deadline = time.monotonic() + 30
            while True:
                count = self._index_count()
                if count == 3:
                    break
                self.assertLess(time.monotonic(), deadline, "Async index did not catch up")
                time.sleep(0.1)

    def _execute(self, query):
        return self.pool.retry_operation_sync(lambda session: session.transaction().execute(query, commit_tx=True))

    def _index_count(self):
        return self.pool.retry_operation_sync(lambda session: session.transaction(ydb.StaleReadOnly()).execute(
            f"SELECT COUNT(*) AS n FROM `{self.index}`;", commit_tx=True
        ))[0].rows[0]["n"]

    def _run(self, table=None, ddl_excluded=True):
        command = [yatest.common.binary_path("ydb/tools/validate_indexes/bin/validate-indexes"),
                   "--endpoint", self.endpoint, "--database", "/Root", "--table", table or self.table]
        if ddl_excluded:
            command.append("--ddl-excluded")
        result = subprocess.run(
            command,
            capture_output=True, text=True, timeout=90,
        )
        return result.returncode, result.stdout, result.stderr

    def _corrupt(self, path, operation):
        # Administrative writes bypass index maintenance, only on this fixture's tablets.
        request = msgbus_pb2.TSchemeDescribe(Path=path)
        request.Options.ReturnPartitioningInfo = True
        description = self.cluster.client.send(request, "SchemeDescribe").PathDescription
        self.assertEqual(len(description.TablePartitions), 1)
        tablet_id = description.TablePartitions[0].DatashardId
        response = self.cluster.client.tablet_service.ExecuteTabletMiniKQL(
            ydb_tablet_pb2.ExecuteTabletMiniKQLRequest(
                tablet_id=tablet_id,
                program=f"((return (AsList {operation})))",
            )
        )
        self.assertEqual(response.status, ydb.StatusCode.SUCCESS, str(response))

    def _erase_index(self):
        self._corrupt(self.index, """(EraseRow '__user__indexImplTable
            '('('value (Utf8 'first)) '('tenant (Utf8 'a)) '('id (Uint64 '1))))""")

    def _erase_main(self):
        name = self.table.rsplit("/", 1)[1]
        self._corrupt(self.table, f"""(EraseRow '__user__{name}
            '('('tenant (Utf8 'a)) '('id (Uint64 '1))))""")

    def test_ddl_exclusion_is_required(self):
        self._create()
        code, output, error = self._run(ddl_excluded=False)
        self.assertEqual(code, 2, output + error)
        self.assertIn("DDL", error)
        self.assertNotIn("consistent", output)
        with self.pool.checkout() as session, self.assertRaisesRegex(ValueError, "DDL exclusion"):
            validator.validate(self.driver, session, self.table)

    def test_changefeed_progress_is_not_schema(self):
        self._create()
        with self.pool.checkout() as session:
            description = validator._describe(self.driver, session, self.table)
        feed_type = validator._apis.ydb_table.ChangefeedDescription
        feed = description.changefeeds.add(name="events", state=feed_type.STATE_INITIAL_SCAN)
        feed.initial_scan_progress.parts_total = 2
        feed.initial_scan_progress.parts_completed = 0
        expected = validator.Table._schema(description)
        # Initial scanning progresses and becomes enabled without user DDL.
        feed.initial_scan_progress.parts_completed = 1
        self.assertEqual(validator.Table._schema(description), expected)
        feed.state = feed_type.STATE_ENABLED
        feed.ClearField("initial_scan_progress")
        self.assertEqual(validator.Table._schema(description), expected)

    def test_schema_add_index_race(self):
        self._create()
        self._erase_index()
        original_describe = validator._describe
        changed = []

        def create_corrupt_index_then_repair_original(driver, session, path):
            description = original_describe(driver, session, path)
            if path == self.table and not changed:
                changed.append(True)
                self.pool.retry_operation_sync(lambda ddl_session: ddl_session.execute_scheme(
                    f"ALTER TABLE `{self.table}` ADD INDEX by_payload GLOBAL ON (payload);"
                ))
                self._corrupt(self.table + "/by_payload/indexImplTable", """(EraseRow '__user__indexImplTable
                    '('('payload (Utf8 'one)) '('tenant (Utf8 'a)) '('id (Uint64 '1))))""")
                self._corrupt(self.index, """(UpdateRow '__user__indexImplTable
                    '('('value (Utf8 'first)) '('tenant (Utf8 'a)) '('id (Uint64 '1)))
                    '('('payload (Utf8 'one))))""")
            return description

        output = io.StringIO()
        error = io.StringIO()
        with mock.patch.object(validator, "_describe", create_corrupt_index_then_repair_original):
            with contextlib.redirect_stdout(output), contextlib.redirect_stderr(error):
                code = validator.main(["--endpoint", self.endpoint, "--database", "/Root",
                                       "--table", self.table, "--ddl-excluded"])
        self.assertEqual(code, 2, output.getvalue() + error.getvalue())
        self.assertIn("Schema changed", error.getvalue())
        self.assertTrue(changed)
        self.assertEqual(self._index_count(), 3)
        remaining = self._execute(f"SELECT COUNT(*) AS n FROM `{self.table}/by_payload/indexImplTable`;")
        self.assertEqual(remaining[0].rows[0]["n"], 2)
        self.assertNotIn("consistent", output.getvalue())

    def test_schema_add_first_index_race(self):
        self.table = f"/Root/first_index_{uuid.uuid4().hex}"
        self.pool.retry_operation_sync(lambda session: session.execute_scheme(
            f"CREATE TABLE `{self.table}` (id Uint64, value Utf8, PRIMARY KEY (id));"
        ))
        original_describe = validator._describe
        changed = []

        def add_first_index(driver, session, path):
            description = original_describe(driver, session, path)
            if path == self.table and not changed:
                changed.append(True)
                self.pool.retry_operation_sync(lambda ddl_session: ddl_session.execute_scheme(
                    f"ALTER TABLE `{self.table}` ADD INDEX by_value GLOBAL ON (value);"
                ))
            return description

        output = io.StringIO()
        with self.pool.checkout() as session, mock.patch.object(validator, "_describe", add_first_index):
            with contextlib.redirect_stdout(output), self.assertRaisesRegex(ValueError, "[Ss]chema changed"):
                validator.validate(self.driver, session, self.table, ddl_excluded=True)
        self.assertTrue(changed)
        self.assertNotIn("No supported indexes", output.getvalue())

    def test_schema_recreate_index_same_name(self):
        self._create()
        original_scan = self.driver.table_client.scan_query
        incarnations = []

        def recreate_index(query, *args, **kwargs):
            with self.pool.checkout() as session:
                incarnations.append(validator._describe(self.driver, session, self.index).self.created_at)
            self.pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
                ALTER TABLE `{self.table}` DROP INDEX by_value;
                ALTER TABLE `{self.table}` ADD INDEX by_value GLOBAL ON (value, tenant) COVER (payload);
            """))
            with self.pool.checkout() as session:
                incarnations.append(validator._describe(self.driver, session, self.index).self.created_at)
            self.assertNotEqual(incarnations[0], incarnations[1], "Fixture must recreate the implementation table")
            return original_scan(query, *args, **kwargs)

        output = io.StringIO()
        with self.pool.checkout() as session:
            with mock.patch.object(self.driver.table_client, "scan_query", recreate_index):
                with contextlib.redirect_stdout(output), self.assertRaisesRegex(ValueError, "[Ss]chema changed"):
                    validator.validate(self.driver, session, self.table, ddl_excluded=True)
        self.assertEqual(len(incarnations), 2)
        self.assertNotEqual(incarnations[0], incarnations[1], "Fixture must recreate the implementation table")
        self.assertNotIn("consistent", output.getvalue())

    def test_schema_recreate_table_same_name(self):
        self._create()
        original_scan = self.driver.table_client.scan_query
        recreated = []

        def recreate_table(query, *args, **kwargs):
            self.pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
                DROP TABLE `{self.table}`;
                CREATE TABLE `{self.table}` (
                    tenant Utf8, id Uint64, value Utf8, payload Utf8, PRIMARY KEY (tenant, id),
                    INDEX by_value GLOBAL SYNC ON (value, tenant) COVER (payload)
                );
            """))
            recreated.append(True)
            return original_scan(query, *args, **kwargs)

        output = io.StringIO()
        with self.pool.checkout() as session:
            with mock.patch.object(self.driver.table_client, "scan_query", recreate_table):
                with contextlib.redirect_stdout(output), self.assertRaisesRegex(ValueError, "[Ss]chema changed"):
                    validator.validate(self.driver, session, self.table, ddl_excluded=True)
        self.assertTrue(recreated)
        self.assertNotIn("consistent", output.getvalue())

    def _floating_table(self, kind, value):
        self.table = f"/Root/floating_{uuid.uuid4().hex}"
        self.index = self.table + "/by_value/indexImplTable"
        self.pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
            CREATE TABLE `{self.table}` (
                id Uint64, value Uint64, score {kind}, PRIMARY KEY (id),
                INDEX by_value GLOBAL ON (value) COVER (score)
            );
        """))
        self._execute(f"UPSERT INTO `{self.table}` (id, value, score) VALUES (1u, 2u, {value});")

    def test_floating_signed_zero_mismatch(self):
        for kind, negative_bits in (("Float", "00000080"), ("Double", "0000000000000080")):
            with self.subTest(kind=kind):
                self._floating_table(kind, f"{kind}('0')")
                code, output, error = self._run()
                self.assertEqual(code, 0, output + error)
                self._corrupt(self.index, f"""(UpdateRow '__user__indexImplTable
                    '('('value (Uint64 '2)) '('id (Uint64 '1)))
                    '('('score ({kind} '-0))))""")
                stored = self._execute(f"SELECT ToBytes(score) AS bits FROM `{self.index}`;")
                self.assertEqual(stored[0].rows[0]["bits"].hex(), negative_bits)
                code, output, error = self._run()
                self.assertEqual(code, 1, output + error)
                differences = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
                self.assertEqual(len(differences), 2, output)
                self.assertEqual({row["pk"]["id"] for row in differences}, {1})
                self.assertEqual({math.copysign(1, row["row"]["score"]) for row in differences}, {-1, 1})
                self.assertEqual({row["comparison_bytes"]["score"] for row in differences},
                                 {"0" * len(negative_bits), negative_bits})

    def test_floating_nan_payload_mismatch(self):
        for kind, original_bits in (("Float", "0100c07f"), ("Double", "010000000000f87f")):
            with self.subTest(kind=kind):
                self._floating_table(kind, f"{kind}('0')")
                score = struct.unpack("<f" if kind == "Float" else "<d", bytes.fromhex(original_bits))[0]
                query = ydb.DataQuery(
                    f"DECLARE $score AS {kind}; UPSERT INTO `{self.table}` (id, value, score) VALUES (1u, 2u, $score);",
                    {"$score": getattr(ydb.PrimitiveType, kind)},
                )
                self.pool.retry_operation_sync(lambda session: session.transaction().execute(
                    query, {"$score": score}, commit_tx=True
                ))
                stored = self._execute(f"SELECT ToBytes(score) AS bits FROM `{self.table}`;")
                self.assertEqual(stored[0].rows[0]["bits"].hex(), original_bits)
                code, output, error = self._run()
                self.assertEqual(code, 0, output + error)
                self._corrupt(self.index, f"""(UpdateRow '__user__indexImplTable
                    '('('value (Uint64 '2)) '('id (Uint64 '1)))
                    '('('score ({kind} 'nan))))""")
                stored = self._execute(f"SELECT ToBytes(score) AS bits FROM `{self.index}`;")
                actual_bits = stored[0].rows[0]["bits"].hex()
                self.assertNotEqual(actual_bits, original_bits)
                code, output, error = self._run()
                self.assertEqual(code, 1, output + error)
                differences = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
                self.assertEqual(len(differences), 2, output)
                self.assertTrue(all(row["row"]["score"] == "nan" for row in differences), output)
                self.assertEqual({row["comparison_bytes"]["score"] for row in differences}, {original_bits, actual_bits})

    def test_consistent_sync_and_async(self):
        for kind in ("SYNC", "ASYNC"):
            with self.subTest(kind=kind):
                self._create(kind)
                code, output, error = self._run()
                self.assertEqual(code, 3 if kind == "ASYNC" else 0, output + error)
                self.assertIn("inconclusive" if kind == "ASYNC" else "consistent", output)
                self.assertIn("by_value", output)

    def test_both_empty(self):
        for kind in ("SYNC", "ASYNC"):
            with self.subTest(kind=kind):
                self._create(kind, rows=False)
                code, output, error = self._run()
                self.assertEqual(code, 3 if kind == "ASYNC" else 0, output + error)
                self.assertIn("inconclusive" if kind == "ASYNC" else "consistent", output)
                self.assertFalse(any(line.startswith("{") for line in output.splitlines()), output)

    def test_all_null_row_and_empty_side(self):
        for side in ("main", "index"):
            with self.subTest(side=side):
                self._create(rows=False)
                self._execute(f"UPSERT INTO `{self.table}` (tenant, id, value, payload) VALUES (NULL, NULL, NULL, NULL);")
                code, output, error = self._run()
                self.assertEqual(code, 0, output + error)
                path = self.table if side == "main" else self.index
                name = path.rsplit("/", 1)[1]
                keys = [("tenant", "Utf8"), ("id", "Uint64")]
                if side == "index":
                    keys.insert(0, ("value", "Utf8"))
                key = "'(" + " ".join(
                    f"'('{column} (Nothing (OptionalType (DataType '{kind}))))" for column, kind in keys
                ) + ")"
                self._corrupt(path, f"(EraseRow '__user__{name} {key})")
                code, output, error = self._run()
                self.assertEqual(code, 1, output + error)
                differences = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
                self.assertEqual(len(differences), 1, output)
                self.assertEqual(differences[0]["pk"], {"tenant": None, "id": None})
                self.assertEqual(differences[0]["reason"], "extra index row" if side == "main" else "missing index row")

    def test_missing_index_row(self):
        self._create()
        self._erase_index()
        code, output, error = self._run()
        self.assertEqual(code, 1, output + error)
        self.assertIn("missing", output)
        self.assertIn('"id": 1', output)
        self.assertIn('"tenant": "a"', output)

    def test_extra_index_row(self):
        self._create()
        self._erase_main()
        code, output, error = self._run()
        self.assertEqual(code, 1, output + error)
        self.assertIn("extra", output)
        self.assertIn('"id": 1', output)

    def test_wrong_covering_value(self):
        self._create()
        self._corrupt(self.index, """(UpdateRow '__user__indexImplTable
            '('('value (Utf8 'first)) '('tenant (Utf8 'a)) '('id (Uint64 '1)))
            '('('payload (Utf8 'broken))))""")
        code, output, error = self._run()
        self.assertEqual(code, 1, output + error)
        self.assertIn("payload", output)
        self.assertIn("broken", output)
        self.assertIn('"id": 1', output)

    def test_wrong_index_key(self):
        self._create()
        self._erase_index()
        self._corrupt(self.index, """(UpdateRow '__user__indexImplTable
            '('('value (Utf8 'wrong)) '('tenant (Utf8 'a)) '('id (Uint64 '1)))
            '('('payload (Utf8 'one))))""")
        code, output, error = self._run()
        self.assertEqual(code, 1, output + error)
        self.assertIn("missing", output)
        self.assertIn("wrong", output)
        self.assertIn('"id": 1', output)

    def test_empty_index(self):
        for kind in ("SYNC", "ASYNC"):
            with self.subTest(kind=kind):
                self._create(kind, rows=False)
                self._execute(f"UPSERT INTO `{self.table}` (tenant, id, value, payload) VALUES ('a', 1u, 'first', 'one');")
                deadline = time.monotonic() + 30
                while self._index_count() != 1:
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.1)
                self._erase_index()
                code, output, error = self._run()
                self.assertEqual(code, 3 if kind == "ASYNC" else 1, output + error)
                self.assertIn("missing", output)
                self.assertIn('"id": 1', output)

    def test_empty_main(self):
        for kind in ("SYNC", "ASYNC"):
            with self.subTest(kind=kind):
                self._create(kind, rows=False)
                self._execute(f"UPSERT INTO `{self.table}` (tenant, id, value, payload) VALUES ('a', 1u, 'first', 'one');")
                deadline = time.monotonic() + 30
                while self._index_count() != 1:
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.1)
                self._erase_main()
                code, output, error = self._run()
                self.assertEqual(code, 3 if kind == "ASYNC" else 1, output + error)
                self.assertIn("extra", output)
                self.assertIn('"id": 1', output)

    def test_table_without_indexes(self):
        self.table = f"/Root/no_indexes_{uuid.uuid4().hex}"
        self.pool.retry_operation_sync(lambda session: session.execute_scheme(
            f"CREATE TABLE `{self.table}` (id Uint64, PRIMARY KEY (id));"
        ))
        code, output, error = self._run()
        self.assertEqual(code, 0, output + error)
        self.assertIn("No supported indexes", output)

    def test_multiple_indexes_including_unique(self):
        self._create()
        self.pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
            ALTER TABLE `{self.table}` ADD INDEX by_id GLOBAL UNIQUE ON (id);
        """))
        code, output, error = self._run(self.table.rsplit("/", 1)[1])
        self.assertEqual(code, 0, output + error)
        self.assertIn("by_value: consistent", output)
        self.assertIn("by_id: consistent", output)
        self.assertIn("Checked index set:", output)
        self.assertIn("by_value (global_index)", output)
        self.assertIn("by_id (global_unique_index)", output)
        self._erase_index()
        code, output, error = self._run()
        self.assertEqual(code, 1, output + error)
        self.assertIn("by_value: inconsistent", output)
        self.assertIn("by_id: consistent", output)

    def test_skip_specialized_indexes(self):
        self.table = f"/Root/specialized_{uuid.uuid4().hex}"
        self.pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
            CREATE TABLE `{self.table}` (
                id Uint64 NOT NULL, embedding String, value Utf8, document JsonDocument,
                PRIMARY KEY (id),
                INDEX by_value GLOBAL ON (value),
                INDEX vector_idx GLOBAL USING vector_kmeans_tree ON (embedding)
                WITH (similarity=cosine, vector_type="uint8", vector_dimension=2,
                      levels=1, clusters=2, autoupdate=false),
                INDEX text_idx GLOBAL USING fulltext_plain ON (value)
                WITH (tokenizer=standard, use_filter_lowercase=true),
                INDEX json_idx GLOBAL USING json ON (document)
            );
        """))
        self._execute(f"UPSERT INTO `{self.table}` (id, value) VALUES (1u, 'first');")
        code, output, error = self._run()
        self.assertEqual(code, 0, output + error)
        self.assertIn("Skipping vector_idx", output)
        self.assertIn("Skipping text_idx", output)
        self.assertIn("Skipping json_idx", output)
        self.assertIn("by_value: consistent", output)

    def test_typed_covering_values(self):
        self.table = f"/Root/typed_{uuid.uuid4().hex}"
        self.pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
            CREATE TABLE `{self.table}` (
                id Uint64 NOT NULL, created Timestamp, amount Decimal(22, 9), data String, score Double,
                PRIMARY KEY (id),
                INDEX by_time GLOBAL ON (created) COVER (amount, data, score)
            );
        """))
        self._execute(f"""
            UPSERT INTO `{self.table}` (id, created, amount, data, score) VALUES
                (1u, Timestamp('2026-11-17T00:00:00Z'), Decimal('12.345', 22, 9), '\\xff', CAST('NaN' AS Double));
        """)
        code, output, error = self._run()
        self.assertEqual(code, 0, output + error)
        self.assertIn("by_time: consistent", output)

    def test_missing_table_is_error(self):
        code, output, error = self._run("/Root/no_such_table")
        self.assertEqual(code, 2, output + error)
        self.assertTrue(error)
        self.assertNotIn("consistent", output)

    def test_decimal_nan_is_consistent(self):
        self.table = f"/Root/decimal_nan_{uuid.uuid4().hex}"
        self.pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
            CREATE TABLE `{self.table}` (
                id Uint64, value Uint64, amount Decimal(22, 9), PRIMARY KEY (id),
                INDEX by_value GLOBAL ON (value) COVER (amount)
            );
        """))
        self._execute(f"UPSERT INTO `{self.table}` (id, value, amount) VALUES (1u, 2u, Decimal('NaN', 22, 9));")
        code, output, error = self._run()
        self.assertEqual(code, 0, output + error)

    def test_full_precision_decimal_difference(self):
        self.table = f"/Root/decimal_precision_{uuid.uuid4().hex}"
        self.pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
            CREATE TABLE `{self.table}` (
                id Uint64, value Uint64, amount Decimal(35, 0), PRIMARY KEY (id),
                INDEX by_value GLOBAL ON (value) COVER (amount)
            );
        """))
        self._execute(f"""
            UPSERT INTO `{self.table}` (id, value, amount) VALUES
                (1u, 2u, Decimal('12345678901234567890123456789012345', 35, 0));
        """)
        # The last digit differs; default Python Decimal precision would hide it.
        self._corrupt(self.table + "/by_value/indexImplTable", """(UpdateRow '__user__indexImplTable
            '('('value (Uint64 '2)) '('id (Uint64 '1)))
            '('('amount (Decimal '12345678901234567890123456789012346 '35 '0))))""")
        code, output, error = self._run()
        self.assertEqual(code, 1, output + error)
        self.assertIn("12345678901234567890123456789012345", output)
        self.assertIn("12345678901234567890123456789012346", output)

    def test_unrelated_postgresql_column(self):
        self.table = f"/Root/pg_payload_{uuid.uuid4().hex}"
        self.pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
            CREATE TABLE `{self.table}` (
                id Uint64, value Utf8, unrelated pgint4, PRIMARY KEY (id),
                INDEX by_value GLOBAL ON (value)
            );
        """))
        self._execute(f"UPSERT INTO `{self.table}` (id, value, unrelated) VALUES (1u, 'first', 42p);")
        code, output, error = self._run()
        self.assertEqual(code, 0, output + error)

    def test_json_covering_values(self):
        self.table = f"/Root/json_cover_{uuid.uuid4().hex}"
        self.pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
            CREATE TABLE `{self.table}` (
                id Uint64, value Utf8, document JsonDocument, text Json, PRIMARY KEY (id),
                INDEX by_value GLOBAL ON (value) COVER (document, text)
            );
        """))
        self._execute(f"""
            UPSERT INTO `{self.table}` (id, value, document, text) VALUES
                (1u, 'first', JsonDocument('{{"a": 1}}'), Json('{{"a": 1}}'));
        """)
        code, output, error = self._run()
        self.assertEqual(code, 0, output + error)
        self._corrupt(self.table + "/by_value/indexImplTable", """(UpdateRow '__user__indexImplTable
            '('('value (Utf8 'first)) '('id (Uint64 '1)))
            '('('text (Json 'null))))""")
        code, output, error = self._run()
        self.assertEqual(code, 1, output + error)
        self.assertIn('"id": 1', output)

    def test_postgresql_indexed_values(self):
        self.table = f"/Root/pg_indexed_{uuid.uuid4().hex}"
        self.pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
            CREATE TABLE `{self.table}` (
                id Uint64, value pgint4, payload pgtext, amount pgnumeric, PRIMARY KEY (id),
                INDEX by_value GLOBAL ON (value) COVER (payload, amount)
            );
        """))
        self._execute(f"""
            UPSERT INTO `{self.table}` (id, value, payload, amount) VALUES
                (1u, 42p, 'original'p, PgNumeric('1234567890123456789012345678901234567890.123456789'));
        """)
        code, output, error = self._run()
        self.assertEqual(code, 0, output + error)
        name = self.table.rsplit("/", 1)[1]
        self._corrupt(self.table, f"(EraseRow '__user__{name} '('('id (Uint64 '1))))")
        code, output, error = self._run()
        self.assertEqual(code, 1, output + error)
        differences = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
        self.assertEqual(len(differences), 1, output + error)
        self.assertEqual(differences[0]["pk"], {"id": 1})
        self.assertEqual(differences[0]["index_key"]["value"], "42")
        self.assertEqual(differences[0]["row"]["amount"], "1234567890123456789012345678901234567890.123456789")

    def test_postgresql_primary_key(self):
        self.table = f"/Root/pg_key_{uuid.uuid4().hex}"
        self.pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
            CREATE TABLE `{self.table}` (
                id pgint4, value pgtext, payload pgtext, PRIMARY KEY (id),
                INDEX by_value GLOBAL ON (value) COVER (payload)
            );
        """))
        self._execute(f"""
            UPSERT INTO `{self.table}` (id, value, payload) VALUES
                (1p, 'key'p, 'original'p), (NULL, 'null-key'p, NULL);
        """)
        code, output, error = self._run()
        self.assertEqual(code, 0, output + error)

    def test_postgresql_char_preserves_trailing_spaces(self):
        self.table = f"/Root/pg_char_{uuid.uuid4().hex}"
        self.pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
            CREATE TABLE `{self.table}` (
                id Uint64, value Utf8, payload pgbpchar, PRIMARY KEY (id),
                INDEX by_value GLOBAL ON (value) COVER (payload)
            );
        """))
        self._execute(f"UPSERT INTO `{self.table}` (id, value, payload) VALUES (1u, 'first', PgBpChar('x  '));")
        code, output, error = self._run()
        self.assertEqual(code, 0, output + error)
        name = self.table.rsplit("/", 1)[1]
        self._corrupt(self.table, f"(EraseRow '__user__{name} '('('id (Uint64 '1))))")
        code, output, error = self._run()
        self.assertEqual(code, 1, output + error)
        differences = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
        self.assertEqual(len(differences), 1, output + error)
        self.assertEqual(differences[0]["row"]["payload"], "x  ")

    def _hot_table(self, asynchronous=False, partitioned=False, rows=130):
        self.table = f"/Root/hot_{uuid.uuid4().hex}"
        async_index = ", INDEX by_async GLOBAL ASYNC ON (value)" if asynchronous else ""
        self.pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
            CREATE TABLE `{self.table}` (
                id Uint64 NOT NULL, value Uint64, payload Utf8,
                PRIMARY KEY (id),
                INDEX by_value GLOBAL ON (value) COVER (payload),
                INDEX by_payload GLOBAL ON (payload) COVER (value)
                {async_index}
            ) {"WITH (PARTITION_AT_KEYS = (8, 16, 32, 64, 96))" if partitioned else ""};
        """))
        self._execute(f"UPSERT INTO `{self.table}` (id, value, payload) VALUES " +
                      ",".join(f"({key}u, {key}u, 'initial')" for key in range(rows)) + ";")

    @contextlib.contextmanager
    def _writer(self):
        stop = threading.Event()
        started = threading.Event()
        errors = []
        writes = []
        table_path = self.table

        def write():
            generation = 0
            try:
                while not stop.is_set():
                    # Preserve id=0 so corruption there remains present during the load.
                    values = ",".join(f"({key}u, {generation % 3}u, 'generation-{generation}')" for key in range(1, 24))
                    self._execute(f"""
                        DELETE FROM `{table_path}` WHERE id >= 24u AND id < 40u;
                        UPSERT INTO `{table_path}` (id, value, payload) VALUES {values},
                            ({24 + generation % 16}u, {generation}u, 'new');
                    """)
                    writes.append(generation)
                    started.set()
                    generation += 1
            except BaseException as error:
                errors.append(error)
                started.set()

        writer = threading.Thread(target=write, daemon=True)
        writer.start()
        self.assertTrue(started.wait(30), "Writer did not start")
        try:
            yield writes
        finally:
            stop.set()
            writer.join(30)
            self.assertFalse(writer.is_alive(), "Writer did not stop")
            if errors:
                raise errors[0]

    def test_hot_table_cli(self):
        self._hot_table()
        with self._writer() as writes:
            before = len(writes)
            code, output, error = self._run()
            self.assertEqual(code, 0, output + error)
            self.assertGreater(len(writes), before + 1, "Writes did not overlap validation")

    def test_writes_overlap_query_execution(self):
        self._hot_table(rows=4096, partitioned=True)
        original_call = ydb.Driver.__call__
        query_writes = []
        with self._writer() as writes:
            def record_query_start(driver, request, stub, method, *args, **kwargs):
                if method == "StreamExecuteScanQuery":
                    query_writes.append(len(writes))
                return original_call(driver, request, stub, method, *args, **kwargs)

            with self.pool.checkout() as session, mock.patch.object(ydb.Driver, "__call__", record_query_start):
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertTrue(validator.validate(self.driver, session, self.table, ddl_excluded=True))
            self.assertEqual(len(query_writes), 1)
            self.assertGreater(len(writes), query_writes[0] + 1, "Writes must commit during the query itself")

    def test_hot_table_with_persistent_corruption(self):
        self._hot_table()
        self._corrupt(self.table + "/by_value/indexImplTable", """(EraseRow '__user__indexImplTable
            '('('value (Uint64 '0)) '('id (Uint64 '0))))""")
        with self._writer() as writes:
            before = len(writes)
            code, output, error = self._run()
            self.assertEqual(code, 1, output + error)
            self.assertIn('"id": 0', output)
            self.assertGreater(len(writes), before + 1, "Writes did not overlap validation")

    def test_async_result_is_inconclusive(self):
        self._create("ASYNC")
        code, output, error = self._run()
        self.assertEqual(code, 3, output + error)
        self.assertIn("inconclusive", output)
        self.assertNotIn("by_value: consistent", output)

    def test_hot_table_async_is_inconclusive(self):
        self._hot_table(asynchronous=True)
        with self._writer() as writes:
            before = len(writes)
            code, output, error = self._run()
            self.assertEqual(code, 3, output + error)
            self.assertIn("by_value: consistent", output)
            self.assertIn("by_payload: consistent", output)
            self.assertIn("by_async: inconclusive", output)
            self.assertGreater(len(writes), before + 1)

    def test_sync_corruption_wins_over_inconclusive_async(self):
        self._hot_table(asynchronous=True)
        self._corrupt(self.table + "/by_value/indexImplTable", """(EraseRow '__user__indexImplTable
            '('('value (Uint64 '0)) '('id (Uint64 '0))))""")
        with self._writer():
            code, output, error = self._run()
        self.assertEqual(code, 1, output + error)
        self.assertIn("by_value: inconsistent", output)
        self.assertIn("by_async: inconclusive", output)

    def test_all_indexes_use_one_read_request(self):
        self._hot_table(asynchronous=True)
        original_call = ydb.Driver.__call__
        reads = []

        def record_reads(driver, request, stub, method, *args, **kwargs):
            if method in {"ExecuteDataQuery", "StreamReadTable", "StreamExecuteScanQuery", "ExecuteQuery"}:
                reads.append(request)
            return original_call(driver, request, stub, method, *args, **kwargs)

        output = io.StringIO()
        with self.pool.checkout() as session, mock.patch.object(ydb.Driver, "__call__", record_reads):
            with contextlib.redirect_stdout(output):
                self.assertIsNone(validator.validate(self.driver, session, self.table, ddl_excluded=True))
        self.assertIn("by_value: consistent", output.getvalue())
        self.assertIn("by_payload: consistent", output.getvalue())
        self.assertIn("by_async: inconclusive", output.getvalue())
        self.assertEqual(len(reads), 1, "Validation must use one server-side query for all indexes")

    def test_failed_stream_is_not_restarted(self):
        self._create()
        original_next = ydb._utilities.SyncResponseIterator.__next__
        responses = []

        def fail_after_response(stream):
            if responses:
                raise ydb.Unavailable("Validation stream was interrupted")
            response = original_next(stream)
            responses.append(response)
            return response

        output = io.StringIO()
        with self.pool.checkout() as session:
            with mock.patch.object(ydb._utilities.SyncResponseIterator, "__next__", fail_after_response):
                with contextlib.redirect_stdout(output), self.assertRaises(ydb.Unavailable):
                    validator.validate(self.driver, session, self.table, ddl_excluded=True)
        self.assertTrue(responses, "A real server response must precede the injected interruption")
        self.assertNotIn("consistent", output.getvalue())

    def test_truncated_stream_is_error(self):
        self._create()
        original_next = ydb._utilities.SyncResponseIterator.__next__

        def truncate_response(stream):
            response = original_next(stream)
            response.result_set.truncated = True
            return response

        output = io.StringIO()
        with self.pool.checkout() as session:
            with mock.patch.object(ydb._utilities.SyncResponseIterator, "__next__", truncate_response):
                with contextlib.redirect_stdout(output), self.assertRaises(ydb.TruncatedResponseError):
                    validator.validate(self.driver, session, self.table, ddl_excluded=True)
        self.assertNotIn("consistent", output.getvalue())

    def test_hot_partitioned_table(self):
        self._hot_table(partitioned=True)
        with self._writer() as writes:
            before = len(writes)
            code, output, error = self._run()
            self.assertEqual(code, 0, output + error)
            self.assertGreater(len(writes), before + 1)

    def test_consistent_then_corrupt_then_repaired_during_writes(self):
        self._hot_table()
        with self._writer() as writes:
            before = len(writes)
            code, output, error = self._run()
            self.assertEqual(code, 0, output + error)
            self._corrupt(self.table + "/by_value/indexImplTable", """(EraseRow '__user__indexImplTable
                '('('value (Uint64 '0)) '('id (Uint64 '0))))""")
            code, output, error = self._run()
            self.assertEqual(code, 1, output + error)
            differences = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
            self.assertTrue(differences, output)
            self.assertEqual({row["pk"]["id"] for row in differences}, {0}, output)
            self.assertIn("by_payload: consistent", output)
            self._execute(f"UPSERT INTO `{self.table}` (id, value, payload) VALUES (0u, 999u, 'repaired');")
            code, output, error = self._run()
            self.assertEqual(code, 0, output + error)
            self.assertGreater(len(writes), before + 3)

    def test_all_extra_rows_are_reported(self):
        self._create(rows=False)
        values = ",".join(f"('a', {key}u, 'first', 'payload')" for key in range(1050))
        self._execute(f"UPSERT INTO `{self.table}` (tenant, id, value, payload) VALUES {values};")
        name = self.table.rsplit("/", 1)[1]
        self._corrupt(self.table, " ".join(
            f"(EraseRow '__user__{name} '('('tenant (Utf8 'a)) '('id (Uint64 '{key}))))"
            for key in range(1050)
        ))
        code, output, error = self._run()
        self.assertEqual(code, 1, output + error)
        differences = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
        self.assertEqual({row["pk"]["id"] for row in differences}, set(range(1050)), output + error)
        self.assertTrue(all("extra" in row["reason"] for row in differences))

    def test_nullable_composite_keys(self):
        self._create(rows=False)
        values = []
        for key in range(130):
            tenant, value = ("NULL", "NULL") if key < 65 else ("'a'", "'next'")
            values.append(f"({tenant}, {key}u, {value}, 'payload-{key}')")
        self._execute(f"UPSERT INTO `{self.table}` (tenant, id, value, payload) VALUES " + ",".join(values) + ";")
        code, output, error = self._run()
        self.assertEqual(code, 0, output + error)
        self.assertIn("by_value: consistent", output)

    def test_decimal_nan_keys(self):
        self.table = f"/Root/nan_keys_{uuid.uuid4().hex}"
        self.pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
            CREATE TABLE `{self.table}` (
                id Decimal(22, 9), seq Uint64, indexed Decimal(22, 9), payload Utf8,
                PRIMARY KEY (id, seq),
                INDEX by_id GLOBAL ON (indexed) COVER (payload)
            );
        """))
        values = [f"(Decimal('0', 22, 9), {key}u, Decimal('0', 22, 9), 'finite')" for key in range(65)]
        values.extend(f"(Decimal('NaN', 22, 9), {key}u, Decimal('NaN', 22, 9), 'nan')" for key in range(70))
        self._execute(f"UPSERT INTO `{self.table}` (id, seq, indexed, payload) VALUES " + ",".join(values) + ";")
        code, output, error = self._run()
        self.assertEqual(code, 0, output + error)
        self.assertIn("by_id: consistent", output)

    def test_missing_decimal_nan_key(self):
        self.table = f"/Root/missing_nan_{uuid.uuid4().hex}"
        self.pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
            CREATE TABLE `{self.table}` (
                id Decimal(22, 9), indexed Decimal(22, 9), PRIMARY KEY (id),
                INDEX by_id GLOBAL ON (indexed)
            );
        """))
        values = [f"(Decimal('{key}', 22, 9), Decimal('{key}', 22, 9))" for key in range(64)]
        values.append("(Decimal('NaN', 22, 9), Decimal('NaN', 22, 9))")
        self._execute(f"UPSERT INTO `{self.table}` (id, indexed) VALUES " + ",".join(values) + ";")
        self._corrupt(self.table + "/by_id/indexImplTable", """(EraseRow '__user__indexImplTable
            '('('indexed (Decimal 'nan '22 '9)) '('id (Decimal 'nan '22 '9))))""")
        code, output, error = self._run()
        self.assertEqual(code, 1, output + error)
        self.assertIn("Decimal('NaN')", output)
