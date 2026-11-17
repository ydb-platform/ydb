import contextlib
import io
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

    def _run(self, table=None):
        result = subprocess.run(
            [yatest.common.binary_path("ydb/tools/validate_indexes/bin/validate-indexes"),
             "--endpoint", self.endpoint, "--database", "/Root", "--table", table or self.table],
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

    def test_snapshot_survives_changes_between_reads(self):
        self._create()
        original_rows = validator.Table.rows
        changed = False

        def interleaved_rows(table, *args, **kwargs):
            nonlocal changed
            for row in original_rows(table, *args, **kwargs):
                if not changed and table.path == self.table:
                    changed = True
                    self._execute(f"""
                        DELETE FROM `{self.table}` WHERE tenant = 'a' AND id = 1u;
                        UPSERT INTO `{self.table}` (tenant, id, value, payload) VALUES
                            ('a', 2u, 'changed', 'changed'), ('new', 4u, 'inserted', 'inserted');
                    """)
                yield row

        with self.pool.checkout() as session, mock.patch.object(validator.Table, "rows", interleaved_rows):
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertTrue(validator.validate(self.driver, session, self.table))
        self.assertTrue(changed, "Validation did not overlap the independent write")

    def _hot_table(self, asynchronous=False):
        self.table = f"/Root/hot_{uuid.uuid4().hex}"
        async_index = ", INDEX by_async GLOBAL ASYNC ON (value)" if asynchronous else ""
        self.pool.retry_operation_sync(lambda session: session.execute_scheme(f"""
            CREATE TABLE `{self.table}` (
                id Uint64 NOT NULL, value Uint64, payload Utf8,
                PRIMARY KEY (id),
                INDEX by_value GLOBAL ON (value) COVER (payload),
                INDEX by_payload GLOBAL ON (payload) COVER (value)
                {async_index}
            );
        """))
        self._execute(f"UPSERT INTO `{self.table}` (id, value, payload) VALUES " +
                      ",".join(f"({key}u, {key}u, 'initial')" for key in range(130)) + ";")

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

    def test_same_snapshot_across_indexes(self):
        self._hot_table()
        original_check = validator._check_index
        changed = False

        def interleaved_check(*args, **kwargs):
            nonlocal changed
            result = original_check(*args, **kwargs)
            if not changed:
                changed = True
                self._execute(f"""
                    DELETE FROM `{self.table}`;
                    UPSERT INTO `{self.table}` (id, value, payload) VALUES (999u, 999u, 'replacement');
                """)
            return result

        output = io.StringIO()
        with self.pool.checkout() as session, mock.patch.object(validator, "_check_index", interleaved_check):
            with contextlib.redirect_stdout(output):
                self.assertTrue(validator.validate(self.driver, session, self.table))
        self.assertTrue(changed)
        self.assertEqual(output.getvalue().count("table rows: 130, index rows: 130"), 2, output.getvalue())

    def test_lost_snapshot_is_not_restarted(self):
        self._create()
        session = self.driver.table_client.session().create()
        self.addCleanup(lambda: session.delete() if session.initialized() else None)
        original_rows = validator.Table.rows
        deleted = False

        def lose_session(table, *args, **kwargs):
            nonlocal deleted
            for row in original_rows(table, *args, **kwargs):
                if not deleted:
                    deleted = True
                    session.delete()
                yield row

        output = io.StringIO()
        with mock.patch.object(validator.Table, "rows", lose_session), contextlib.redirect_stdout(output):
            with self.assertRaises(ydb.Error):
                validator.validate(self.driver, session, self.table)
        self.assertTrue(deleted)
        self.assertNotIn("consistent", output.getvalue())

    def test_nullable_composite_pagination(self):
        self._create(rows=False)
        values = []
        for key in range(130):
            tenant, value = ("NULL", "NULL") if key < 65 else ("'a'", "'next'")
            values.append(f"({tenant}, {key}u, {value}, 'payload-{key}')")
        self._execute(f"UPSERT INTO `{self.table}` (tenant, id, value, payload) VALUES " + ",".join(values) + ";")
        code, output, error = self._run()
        self.assertEqual(code, 0, output + error)
        self.assertIn("table rows: 130, index rows: 130", output)

    def test_decimal_nan_key_pagination(self):
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
        self.assertIn("table rows: 135, index rows: 135", output)

    def test_missing_decimal_nan_key_after_page_boundary(self):
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
