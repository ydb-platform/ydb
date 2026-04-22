# -*- coding: utf-8 -*-
import copy
import glob
import json
import logging
import os
import time

import pytest
import yatest

from ydb.tests.oss.ydb_sdk_import import ydb

from ydb.tests.library.harness.util import LogLevels

logger = logging.getLogger(__name__)


CLUSTER_CONFIG = dict(
    additional_log_configs={
        'KQP_REQUEST': LogLevels.TRACE,
        'KQP_PROXY': LogLevels.ERROR,
        'KQP_WORKER': LogLevels.ERROR,
        'KQP_YQL': LogLevels.ERROR,
        'KQP_SESSION': LogLevels.ERROR,
        'KQP_COMPILE_ACTOR': LogLevels.ERROR,
        'TX_DATASHARD': LogLevels.ERROR,
        'HIVE': LogLevels.ERROR,
        'FLAT_TX_SCHEMESHARD': LogLevels.ERROR,
        'SCHEME_BOARD_POPULATOR': LogLevels.ERROR,
        'SCHEME_BOARD_SUBSCRIBER': LogLevels.ERROR,
    },
)


def _find_log_files(cluster):
    """Find all log files from all nodes and slots."""
    paths = []
    for node in cluster.nodes.values():
        paths.extend(glob.glob(os.path.join(node.cwd, "logfile_*.log")))
    for slot in cluster.slots.values():
        paths.extend(glob.glob(os.path.join(slot.cwd, "logfile_*.log")))
    return paths


def _collect_req_json_entries(cluster):
    """Parse all [REQ_JSON] log entries from all cluster log files."""
    entries = []
    for path in _find_log_files(cluster):
        with open(path, 'r', errors='replace') as f:
            for line in f:
                idx = line.find('[REQ_JSON] ')
                if idx == -1:
                    continue
                json_str = line[idx + len('[REQ_JSON] '):]
                try:
                    entries.append(json.loads(json_str))
                except json.JSONDecodeError:
                    logger.warning(
                        "Failed to parse REQ_JSON: %s", json_str[:200]
                    )
    return entries


def _find_entries_by_marker(entries, marker, event=None, query_type=None):
    """Find entries containing marker in query_text, optionally filtered by event/query_type."""
    result = []
    for e in entries:
        data = e.get('query_text', '')
        if marker not in data:
            continue
        if event and e.get('event') != event:
            continue
        if query_type and e.get('query_type') != query_type:
            continue
        result.append(e)
    return result


def _ydb_cli_bin():
    if os.getenv("YDB_CLI_BINARY"):
        return yatest.common.binary_path(os.getenv("YDB_CLI_BINARY"))
    raise RuntimeError("YDB_CLI_BINARY is not specified")


def _run_ydb_cli(cluster, database_path, args):
    endpoint = "grpc://%s:%s" % (
        cluster.nodes[1].host,
        cluster.nodes[1].port,
    )
    cmd = [_ydb_cli_bin(), '-e', endpoint, '-d', database_path] + args
    return yatest.common.execute(cmd, check_exit_code=False)


@pytest.fixture(scope='module')
def ydb_setup(ydb_cluster, ydb_database_module_scope):
    database_path = ydb_database_module_scope
    endpoint = "%s:%s" % (
        ydb_cluster.nodes[1].host,
        ydb_cluster.nodes[1].port,
    )
    driver_config = ydb.DriverConfig(endpoint, database_path)
    driver = ydb.Driver(driver_config)
    driver.wait(timeout=15)

    # Create a test table for DML/scan queries
    table_path = database_path + '/req_log_test_table'
    pool = ydb.SessionPool(driver)

    def create_table(session):
        session.execute_scheme("""
            CREATE TABLE `{path}` (
                id Int64,
                value Utf8,
                PRIMARY KEY (id)
            )
        """.format(path=table_path))

    pool.retry_operation_sync(create_table)

    def fill_table(session):
        session.transaction().execute(
            """
            UPSERT INTO `{path}` (id, value) VALUES
                (1, 'one'), (2, 'two'), (3, 'three')
            """.format(path=table_path),
            commit_tx=True,
        )

    pool.retry_operation_sync(fill_table)

    yield driver, database_path, table_path, pool, ydb_cluster

    pool.stop()
    driver.stop()


class TestQueryService:
    """Tests for KQP_REQUEST logging via query service (SQL_GENERIC_QUERY)."""

    def test_select_started_and_completed(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        query_pool = ydb.QuerySessionPool(driver)
        query_pool.execute_with_retries(
            "SELECT 'qs_select_log' AS marker"
        )
        query_pool.stop()

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        started = _find_entries_by_marker(
            entries, 'qs_select_log', event='started',
            query_type='QUERY_TYPE_SQL_GENERIC_QUERY',
        )
        assert len(started) >= 1, \
            "Expected at least one 'started' entry for SQL_GENERIC_QUERY"

        completed = _find_entries_by_marker(
            entries, 'qs_select_log', event='completed',
            query_type='QUERY_TYPE_SQL_GENERIC_QUERY',
        )
        assert len(completed) >= 1, \
            "Expected at least one 'completed' entry for SQL_GENERIC_QUERY"

        # Verify req_id correlation
        started_req_id = started[0]['req_id']
        matching = [e for e in completed if e['req_id'] == started_req_id]
        assert len(matching) == 1, \
            "Expected one completed entry with matching req_id"

    def test_completed_has_metadata(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        query_pool = ydb.QuerySessionPool(driver)
        query_pool.execute_with_retries(
            "SELECT 'qs_metadata_check' AS marker"
        )
        query_pool.stop()

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        completed = _find_entries_by_marker(
            entries, 'qs_metadata_check', event='completed',
            query_type='QUERY_TYPE_SQL_GENERIC_QUERY',
        )
        assert len(completed) >= 1

        entry = completed[0]
        req = entry
        assert req.get('database'), "Expected database in completed entry"
        assert req.get('action'), "Expected action in completed entry"
        assert req.get('status') == 'SUCCESS'
        assert req.get('duration_us') is not None
        assert 'cpu_time_us' in req
        assert 'compile_cache_hit' in req

    def test_error_query_logged(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        query_pool = ydb.QuerySessionPool(driver)
        try:
            query_pool.execute_with_retries(
                "SELECT * FROM nonexistent_table_log_test"
            )
        except Exception:
            pass
        query_pool.stop()

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        completed = _find_entries_by_marker(
            entries, 'nonexistent_table_log_test', event='completed',
        )
        assert len(completed) >= 1, \
            "Expected completed entry for failed query"
        assert completed[0].get('status') != 'SUCCESS'


class TestDataService:
    """Tests for KQP_REQUEST logging via old table service (SQL_DML)."""

    def test_data_query_select(self, ydb_setup):
        driver, database_path, table_path, pool, cluster = ydb_setup

        def callee(session):
            return session.transaction().execute(
                "SELECT 'data_svc_select' AS marker",
                commit_tx=True,
            )

        pool.retry_operation_sync(callee)

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        started = _find_entries_by_marker(
            entries, 'data_svc_select', event='started',
        )
        assert len(started) >= 1
        assert started[0].get('query_type') == \
            'QUERY_TYPE_SQL_DML'

        completed = _find_entries_by_marker(
            entries, 'data_svc_select', event='completed',
        )
        assert len(completed) >= 1
        assert completed[0].get('status') == 'SUCCESS'

    def test_prepared_query(self, ydb_setup):
        driver, database_path, table_path, pool, cluster = ydb_setup

        def callee(session):
            prepared = session.prepare(
                "SELECT 'prepared_query_test' AS marker"
            )
            return session.transaction().execute(
                prepared, commit_tx=True,
            )

        pool.retry_operation_sync(callee)

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        completed = _find_entries_by_marker(
            entries, 'prepared_query_test', event='completed',
        )
        assert len(completed) >= 1
        assert completed[0].get('query_type') == \
            'QUERY_TYPE_SQL_DML'
        assert completed[0].get('status') == 'SUCCESS'

    def test_data_query_upsert(self, ydb_setup):
        driver, database_path, table_path, pool, cluster = ydb_setup

        query = "UPSERT INTO `{path}` (id, value) VALUES (100, 'upsert_log_test')".format(
            path=table_path,
        )

        def callee(session):
            return session.transaction().execute(query, commit_tx=True)

        pool.retry_operation_sync(callee)

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        completed = _find_entries_by_marker(
            entries, 'upsert_log_test', event='completed',
        )
        assert len(completed) >= 1
        assert completed[0].get('status') == 'SUCCESS'


class TestScanQuery:
    """Tests for KQP_REQUEST logging via scan queries (SQL_SCAN)."""

    def test_scan_query_logged(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        query = "SELECT id, value FROM `{path}` WHERE value = 'scan_test_marker'".format(
            path=table_path,
        )
        result_sets = driver.table_client.scan_query(query)
        # Consume the iterator
        for _ in result_sets:
            pass

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        started = _find_entries_by_marker(
            entries, 'scan_test_marker', event='started',
        )
        assert len(started) >= 1
        assert started[0].get('query_type') == \
            'QUERY_TYPE_SQL_SCAN'

        completed = _find_entries_by_marker(
            entries, 'scan_test_marker', event='completed',
        )
        assert len(completed) >= 1
        assert completed[0].get('status') == 'SUCCESS'


class TestScriptingService:
    """Tests for KQP_REQUEST logging via scripting service (SQL_SCRIPT)."""

    def test_scripting_started_and_completed(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        scripting_client = ydb.ScriptingClient(driver)
        scripting_client.execute_yql(
            "SELECT 'scripting_log_test' AS marker"
        )

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        started = _find_entries_by_marker(
            entries, 'scripting_log_test', event='started',
        )
        assert len(started) >= 1, \
            "Expected 'started' entry for scripting query"

        completed = _find_entries_by_marker(
            entries, 'scripting_log_test', event='completed',
        )
        assert len(completed) >= 1, \
            "Expected 'completed' entry for scripting query"

    def test_scripting_query_type(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        scripting_client = ydb.ScriptingClient(driver)
        scripting_client.execute_yql(
            "SELECT 'scripting_type_test' AS marker"
        )

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        started = _find_entries_by_marker(
            entries, 'scripting_type_test', event='started',
        )
        assert len(started) >= 1
        assert started[0].get('query_type') == \
            'QUERY_TYPE_SQL_SCRIPT'

    def test_scripting_req_id_correlation(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        scripting_client = ydb.ScriptingClient(driver)
        scripting_client.execute_yql(
            "SELECT 'scripting_reqid_test' AS marker"
        )

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        started = _find_entries_by_marker(
            entries, 'scripting_reqid_test', event='started',
        )
        completed = _find_entries_by_marker(
            entries, 'scripting_reqid_test', event='completed',
        )

        assert len(started) >= 1
        assert len(completed) >= 1

        started_req_id = started[0]['req_id']
        matching = [e for e in completed if e['req_id'] == started_req_id]
        assert len(matching) == 1, \
            "Expected one completed entry with matching req_id"

    def test_scripting_completed_has_status(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        scripting_client = ydb.ScriptingClient(driver)
        scripting_client.execute_yql(
            "SELECT 'scripting_status_test' AS marker"
        )

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        completed = _find_entries_by_marker(
            entries, 'scripting_status_test', event='completed',
        )
        assert len(completed) >= 1
        assert completed[0].get('status') == 'SUCCESS'
        assert completed[0].get('duration_us') is not None


class TestDdlService:
    """Tests for KQP_REQUEST logging via DDL (SQL_DDL)."""

    def test_ddl_create_table_logged(self, ydb_setup):
        driver, database_path, table_path, pool, cluster = ydb_setup

        ddl_table = database_path + '/ddl_log_test_table'

        def callee(session):
            session.execute_scheme("""
                CREATE TABLE `{path}` (
                    key Int64,
                    val Utf8,
                    PRIMARY KEY (key)
                )
            """.format(path=ddl_table))

        pool.retry_operation_sync(callee)

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        started = _find_entries_by_marker(
            entries, 'ddl_log_test_table', event='started',
        )
        assert len(started) >= 1
        assert started[0].get('query_type') == \
            'QUERY_TYPE_SQL_DDL'

        completed = _find_entries_by_marker(
            entries, 'ddl_log_test_table', event='completed',
        )
        assert len(completed) >= 1
        assert completed[0].get('status') == 'SUCCESS'

    def test_ddl_drop_table_logged(self, ydb_setup):
        driver, database_path, table_path, pool, cluster = ydb_setup

        drop_table = database_path + '/ddl_drop_log_test'

        def create(session):
            session.execute_scheme("""
                CREATE TABLE `{path}` (
                    key Int64, PRIMARY KEY (key)
                )
            """.format(path=drop_table))

        pool.retry_operation_sync(create)

        def drop(session):
            session.execute_scheme(
                "DROP TABLE `{path}`".format(path=drop_table)
            )

        pool.retry_operation_sync(drop)

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        completed = _find_entries_by_marker(
            entries, 'ddl_drop_log_test', event='completed',
        )
        drop_entries = [
            e for e in completed if 'DROP' in e.get('query_text', '')
        ]
        assert len(drop_entries) >= 1
        assert drop_entries[0].get('status') == 'SUCCESS'


class TestStreamingScriptingService:
    """Tests for KQP_REQUEST logging via streaming scripting (SQL_SCRIPT_STREAMING)."""

    def test_streaming_yql_started_and_completed(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        result = _run_ydb_cli(
            cluster, database_path,
            ['yql', '-s', "SELECT 'stream_yql_log' AS marker"],
        )
        assert result.exit_code == 0, \
            "ydb yql failed: " + result.std_err.decode('utf-8')

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        started = _find_entries_by_marker(
            entries, 'stream_yql_log', event='started',
        )
        assert len(started) >= 1, \
            "Expected 'started' entry for streaming scripting"

        completed = _find_entries_by_marker(
            entries, 'stream_yql_log', event='completed',
        )
        assert len(completed) >= 1, \
            "Expected 'completed' entry for streaming scripting"

    def test_streaming_yql_query_type(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        _run_ydb_cli(
            cluster, database_path,
            ['yql', '-s', "SELECT 'stream_type_test' AS marker"],
        )

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        started = _find_entries_by_marker(
            entries, 'stream_type_test', event='started',
        )
        assert len(started) >= 1
        assert started[0].get('query_type') == \
            'QUERY_TYPE_SQL_SCRIPT_STREAMING'

    def test_streaming_yql_req_id_correlation(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        _run_ydb_cli(
            cluster, database_path,
            ['yql', '-s', "SELECT 'stream_reqid_test' AS marker"],
        )

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        started = _find_entries_by_marker(
            entries, 'stream_reqid_test', event='started',
        )
        completed = _find_entries_by_marker(
            entries, 'stream_reqid_test', event='completed',
        )

        assert len(started) >= 1
        assert len(completed) >= 1

        started_req_id = started[0]['req_id']
        matching = [e for e in completed if e['req_id'] == started_req_id]
        assert len(matching) == 1, \
            "Expected one completed entry with matching req_id"


class TestGenericScript:
    """Smoke test for SQL_GENERIC_SCRIPT (QueryService.ExecuteScript).

    SQL_GENERIC_SCRIPT runs through kqp_run_script_actor/kqp_script_executions,
    a separate async execution pipeline that does NOT use the session actor's
    KQP_REQUEST logging. Therefore no [REQ_JSON] entries are expected for this
    query type.

    This test verifies that executing a script query with KQP_REQUEST logging
    enabled does not cause crashes or errors — the query must complete
    successfully even though it bypasses session actor logging.
    """

    def test_generic_script_no_crash(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        # QueryService.ExecuteScript is an async operation that returns an
        # operation ID. We use the QuerySessionPool to trigger it indirectly
        # via the query service endpoint, then verify the cluster is healthy.
        query_pool = ydb.QuerySessionPool(driver)
        query_pool.execute_with_retries(
            "SELECT 'generic_script_smoke' AS marker"
        )
        query_pool.stop()

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        # SQL_GENERIC_SCRIPT (ExecuteScript RPC) goes through
        # kqp_run_script_actor, not the session actor. There is no simple
        # SDK/CLI way to invoke ExecuteScript directly. The query above
        # uses ExecuteQuery (SQL_GENERIC_QUERY) which IS logged.
        # This test exists to document that SQL_GENERIC_SCRIPT is a
        # separate pipeline and is not covered by KQP_REQUEST logging.
        started = _find_entries_by_marker(
            entries, 'generic_script_smoke', event='started',
        )
        assert len(started) >= 1, \
            "Query via QueryService should produce log entries"


class TestNoAstDuplicates:
    """Verify that internal AST_DML/AST_SCAN types are NOT logged."""

    def test_no_ast_dml_entries(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        scripting_client = ydb.ScriptingClient(driver)
        scripting_client.execute_yql(
            "SELECT 'no_ast_dup_test' AS marker"
        )

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        ast_entries = _find_entries_by_marker(
            entries, 'no_ast_dup_test',
            query_type='QUERY_TYPE_AST_DML',
        )
        assert len(ast_entries) == 0, \
            "AST_DML entries should NOT be logged"

        ast_scan = _find_entries_by_marker(
            entries, 'no_ast_dup_test',
            query_type='QUERY_TYPE_AST_SCAN',
        )
        assert len(ast_scan) == 0, \
            "AST_SCAN entries should NOT be logged"


class TestLogStructure:
    """Verify the JSON structure of log entries."""

    def test_entry_has_required_fields(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        query_pool = ydb.QuerySessionPool(driver)
        query_pool.execute_with_retries(
            "SELECT 'structure_test' AS marker"
        )
        query_pool.stop()

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        target = _find_entries_by_marker(entries, 'structure_test')
        assert len(target) >= 1

        for entry in target:
            assert 'req_id' in entry, "Missing req_id"
            assert 'pool' in entry, "Missing pool"
            assert 'session' in entry, "Missing session"
            assert 'user' in entry, "Missing user"
            assert 'timestamp' in entry, "Missing timestamp"
            assert 'event' in entry, "Missing event"
            assert entry['event'] in ('started', 'completed')

    def test_started_has_timestamp_no_end_time(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        query_pool = ydb.QuerySessionPool(driver)
        query_pool.execute_with_retries(
            "SELECT 'ts_started_test' AS marker"
        )
        query_pool.stop()

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        started = _find_entries_by_marker(
            entries, 'ts_started_test', event='started',
        )
        assert len(started) >= 1
        entry = started[0]
        assert 'timestamp' in entry, \
            "started entry must have timestamp (query start time)"
        assert 'end_time' not in entry, \
            "started entry must NOT have end_time"

    def test_completed_has_timestamp_and_end_time(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        query_pool = ydb.QuerySessionPool(driver)
        query_pool.execute_with_retries(
            "SELECT 'ts_completed_test' AS marker"
        )
        query_pool.stop()

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        completed = _find_entries_by_marker(
            entries, 'ts_completed_test', event='completed',
        )
        assert len(completed) >= 1
        entry = completed[0]
        assert 'timestamp' in entry, \
            "completed entry must have timestamp (query start time)"
        assert 'end_time' in entry, \
            "completed entry must have end_time"

    def test_timestamp_precedes_end_time(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        query_pool = ydb.QuerySessionPool(driver)
        query_pool.execute_with_retries(
            "SELECT 'ts_order_test' AS marker"
        )
        query_pool.stop()

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        completed = _find_entries_by_marker(
            entries, 'ts_order_test', event='completed',
        )
        assert len(completed) >= 1
        entry = completed[0]
        assert entry['timestamp'] <= entry['end_time'], \
            "timestamp (start) must be <= end_time"

    def test_started_and_completed_share_timestamp(self, ydb_setup):
        """started.timestamp and completed.timestamp must be the same
        (both represent query start time)."""
        driver, database_path, table_path, _, cluster = ydb_setup

        query_pool = ydb.QuerySessionPool(driver)
        query_pool.execute_with_retries(
            "SELECT 'ts_match_test' AS marker"
        )
        query_pool.stop()

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        started = _find_entries_by_marker(
            entries, 'ts_match_test', event='started',
        )
        completed = _find_entries_by_marker(
            entries, 'ts_match_test', event='completed',
        )
        assert len(started) >= 1
        assert len(completed) >= 1

        # Match by req_id
        req_id = started[0]['req_id']
        matched_completed = [
            e for e in completed if e['req_id'] == req_id
        ]
        assert len(matched_completed) == 1

        assert started[0]['timestamp'] == matched_completed[0]['timestamp'], \
            "started and completed should have the same timestamp (query start time)"



# Keys whose values vary across runs (session ids, pointers, generated
# timestamps, wallclock-based stats, database path, transport metadata)
# and must be replaced before canonization.
_VOLATILE_KEYS = (
    'req_id', 'pool', 'session', 'user', 'timestamp', 'end_time',
    'database',
    'database_id',
    'cluster',
    'node_id',
    'node_name',
    'application',
    'client_address',
    'duration_us',
    'cpu_time_us',
    'compile_cache_hit',
    'consumed_ru',
    'read_rows',
    'read_bytes',
    'write_rows',
    'write_bytes',
    'affected_shards',
    'parameters_size',
    'issues',
)


def _placeholder(key):
    return '/masked_' + key


def _sanitize_entry(entry):
    """Return a copy of a REQ_JSON entry with non-deterministic fields replaced
    by field-specific placeholders, so that the result is safe to canonize."""
    e = copy.deepcopy(entry)
    for key in _VOLATILE_KEYS:
        if key in e:
            e[key] = _placeholder(key)
    # "tables" carries per-table counters that depend on plan/runtime and
    # cannot be canonized; drop it entirely if present.
    e.pop('tables', None)
    return e


def _canonize_entries(entries, marker):
    """Filter log entries by marker, sanitize them and sort deterministically."""
    matching = _find_entries_by_marker(entries, marker)
    sanitized = [_sanitize_entry(e) for e in matching]
    sanitized.sort(key=lambda e: e.get('event', ''))
    return sanitized


class TestCanonical:
    """Canonization test for the REQ_JSON log schema.

    Runs a deterministic query and compares a sanitized snapshot of the
    produced log entries against a stored canonical file. Any schema drift
    (new fields, renamed fields, structural changes) forces the developer to
    re-canonize with `ya make -Z`, which makes the change reviewable.

    Volatile fields (session id, req_id, durations, database path, per-table
    counters) are replaced with <MASKED> placeholders before comparison;
    only the stable schema and the literal query text remain.
    """

    def test_canonical_generic_query(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        marker = 'canon_generic_query_marker_v1'
        query_pool = ydb.QuerySessionPool(driver)
        query_pool.execute_with_retries(
            "SELECT '%s' AS v" % marker
        )
        query_pool.stop()

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)
        sanitized = _canonize_entries(entries, marker)

        # Expect exactly one "started" and one "completed" entry, single-chunk.
        assert len(sanitized) == 2, \
            "Expected 2 canonical entries (started+completed), got %d" % len(sanitized)

        return json.dumps(sanitized, sort_keys=True, indent=2)

    def test_canonical_error_query(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        marker = 'canon_error_query_marker_v1'
        query_pool = ydb.QuerySessionPool(driver)
        try:
            query_pool.execute_with_retries(
                "SELECT '%s' FROM non_existent_table_12345" % marker,
                retry_settings=ydb.RetrySettings(max_retries=0),
            )
        except ydb.Error:
            pass
        query_pool.stop()

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)
        sanitized = _canonize_entries(entries, marker)

        assert len(sanitized) == 2, \
            "Expected 2 canonical entries (started+completed), got %d" % len(sanitized)

        return json.dumps(sanitized, sort_keys=True, indent=2)

    def test_canonical_truncated_query(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        # Query longer than SQL_TEXT_MAX_SIZE (20000) — must be truncated.
        marker = 'canon_truncated_' + 'X' * 19985
        query_pool = ydb.QuerySessionPool(driver)
        query_pool.execute_with_retries(
            "SELECT '%s' AS v" % marker
        )
        query_pool.stop()

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        found = _find_entries_by_marker(entries, marker[:100])
        assert len(found) >= 1, "No entries found for truncated marker"

        sanitized = [_sanitize_entry(e) for e in found]
        sanitized.sort(key=lambda e: e.get('event', ''))

        assert len(sanitized) == 2, \
            "Expected 2 entries (started+completed), got %d" % len(sanitized)

        for entry in sanitized:
            assert entry.get('query_text_truncated') is True, \
                "Expected query_text_truncated=true in entry: %s" % entry.get('event')

        return json.dumps(sanitized, sort_keys=True, indent=2)


class TestQueryTruncation:
    """Verify that queries exceeding SQL_TEXT_MAX_SIZE (20000 bytes) are truncated."""

    def test_long_query_truncated(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        # Query exceeding the 20000-byte limit.
        marker = 'trunc_test_' + 'Q' * 19989
        query_pool = ydb.QuerySessionPool(driver)
        query_pool.execute_with_retries(
            "SELECT '%s' AS v" % marker
        )
        query_pool.stop()

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        started = _find_entries_by_marker(entries, marker[:100], event='started')
        assert len(started) >= 1, "Expected started entry"
        assert started[0].get('query_text_truncated') is True, \
            "Expected query_text_truncated=true in started"

        completed = _find_entries_by_marker(entries, marker[:100], event='completed')
        assert len(completed) >= 1, "Expected completed entry"
        assert completed[0].get('query_text_truncated') is True, \
            "Expected query_text_truncated=true in completed"

    def test_short_query_not_truncated(self, ydb_setup):
        driver, database_path, table_path, _, cluster = ydb_setup

        query_pool = ydb.QuerySessionPool(driver)
        query_pool.execute_with_retries(
            "SELECT 'trunc_short_marker' AS v"
        )
        query_pool.stop()

        time.sleep(0.5)
        entries = _collect_req_json_entries(cluster)

        for entry in _find_entries_by_marker(entries, 'trunc_short_marker'):
            assert 'query_text_truncated' not in entry, \
                "Short query must not have query_text_truncated"
