import subprocess
import time
import unittest
import uuid

import yatest.common

from ydb.core.protos import msgbus_pb2
from ydb.public.api.protos.draft import ydb_tablet_pb2
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb


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

    def _run(self, table=None):
        result = subprocess.run(
            [yatest.common.binary_path("ydb/tools/validate_indexes/bin/validate-indexes"),
             "--endpoint", self.endpoint, "--database", "/Root", "--table", table or self.table],
            capture_output=True, text=True, timeout=30,
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
                program=f"((let change {operation}) (return (AsList change)))",
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

    def test_consistent_sync_and_async(self):
        for kind in ("SYNC", "ASYNC"):
            with self.subTest(kind=kind):
                self._create(kind)
                code, output, error = self._run()
                self.assertEqual(code, 0, output + error)
                self.assertIn("consistent", output)
                self.assertIn("by_value", output)

    def test_both_empty(self):
        for kind in ("SYNC", "ASYNC"):
            with self.subTest(kind=kind):
                self._create(kind, rows=False)
                code, output, error = self._run()
                self.assertEqual(code, 0, output + error)
                self.assertIn("consistent", output)

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
                self.assertEqual(code, 1, output + error)
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
                self.assertEqual(code, 1, output + error)
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
