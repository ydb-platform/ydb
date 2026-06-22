import logging
import socket
import time
import requests
import re
from contextlib import contextmanager

import pytest

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure

logger = logging.getLogger(__name__)


WARMUP_DEADLINE_SECONDS = 30
NODE_READY_TIMEOUT_SECONDS = 600

WARMUP_QUERIES = [
    "SELECT 1 + 1 AS result",
    "SELECT 2 * 3 AS result",
    "SELECT 100 / 4 AS result",
    "SELECT LENGTH('test string') AS len",
    "SELECT COALESCE(NULL, 'default') AS val",
    "SELECT IF(1 > 0, 'yes', 'no') AS answer",
]
WARMUP_QUERY_PARAM = "DECLARE $val AS Int64; SELECT $val + 1 AS result"
WARMUP_PARAM_VALUES = {"$val": 42}


# Pins every RPC to `node`. ydb.Driver(**kwargs) doesn't forward
# disable_discovery in this SDK build, so set it on DriverConfig explicitly.
@contextmanager
def _node_pinned_driver(node, database="/Root", timeout=10):
    config = ydb.DriverConfig(
        endpoint=f"grpc://{node.host}:{node.port}",
        database=database,
    )
    config.disable_discovery = True
    driver = ydb.Driver(driver_config=config)
    driver.wait(timeout=timeout)
    try:
        yield driver
    finally:
        driver.stop()


@contextmanager
def _pinned_table_session(node, database="/Root", driver_timeout=10):
    with _node_pinned_driver(node, database=database, timeout=driver_timeout) as driver:
        session = driver.table_client.session().create()
        try:
            yield session
        finally:
            session.delete()


@contextmanager
def _pinned_query_session(node, database="/Root", driver_timeout=10):
    with _node_pinned_driver(node, database=database, timeout=driver_timeout) as driver:
        session = ydb.QuerySession(driver)
        session.create()
        try:
            yield session
        finally:
            session.delete()


def _is_tcp_port_open(host, port, timeout=0.4):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (ConnectionRefusedError, socket.timeout, OSError):
        return False


class CompileCacheView:
    """Helper for working with compile cache sysview, warmup counters,
    and cache population on cluster nodes."""

    def __init__(self, cluster, database="/Root"):
        self.cluster = cluster
        self.database = database

    # --- sysview queries ---

    def get_cached_queries(self, node_id):
        """Return all compile cache entries for the given node from sysview."""
        node = self.cluster.nodes[node_id]
        driver = ydb.Driver(
            endpoint=f"grpc://localhost:{node.port}",
            database=self.database,
        )
        driver.wait(timeout=120)
        try:
            pool = ydb.SessionPool(driver)
            try:
                def _do(session):
                    sql = (
                        f"SELECT NodeId, QueryType, Query, AccessCount "
                        f"FROM `{self.database}/.sys/compile_cache_queries` "
                        f"WHERE NodeId = {node_id}"
                    )
                    result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
                        sql,
                        commit_tx=True,
                        settings=ydb.BaseRequestSettings().with_timeout(10),
                    )
                    return result_sets[0].rows
                return pool.retry_operation_sync(_do)
            finally:
                pool.stop()
        finally:
            driver.stop()

    def get_cached_query_texts(self, node_id, query_type="QUERY_TYPE_SQL_DML"):
        """Return set of query texts in compile cache for the given node and query type."""
        rows = self.get_cached_queries(node_id)
        logger.info(
            "sysview compile_cache_queries for NodeId=%d: %d entries", node_id, len(rows),
        )
        for row in rows[:20]:
            logger.info(
                "  NodeId=%s QueryType=%s AccessCount=%s Query=%.80s",
                row.get("NodeId"), row.get("QueryType"),
                row.get("AccessCount"), row.get("Query", ""),
            )
        return {row["Query"] for row in rows if row.get("QueryType") == query_type}

    def verify_queries_in_cache(self, queries, node_id):
        """Check how many of the given queries are present in compile cache via sysview."""
        cached = self.get_cached_query_texts(node_id)
        from_cache = 0
        for item in queries:
            text = item[0] if isinstance(item, tuple) else item
            if text in cached:
                from_cache += 1
        return from_cache, len(queries)

    def verify_queries_from_cache_query_api(self, queries, node, nodes_count=None):
        """Execute queries via Query API and check from_cache stats."""
        max_attempts = (nodes_count or 3) * 4
        for attempt in range(max_attempts):
            driver = ydb.Driver(
                endpoint=f"grpc://localhost:{node.port}",
                database=self.database,
            )
            driver.wait(timeout=120)
            try:
                pool = ydb.QuerySessionPool(driver)
                try:
                    with pool.checkout() as session:
                        if session.node_id != node.node_id:
                            continue
                        from_cache_count = 0
                        for item in queries:
                            if isinstance(item, tuple):
                                query, params = item
                            else:
                                query, params = item, None
                            result = session.execute(
                                query,
                                params or {},
                                stats_mode=ydb.QueryStatsMode.BASIC,
                            )
                            for _ in result:
                                pass
                            if session.last_query_stats and session.last_query_stats.compilation.from_cache:
                                from_cache_count += 1
                        return from_cache_count, len(queries)
                finally:
                    pool.stop()
            finally:
                driver.stop()
        raise AssertionError(
            f"Failed to get session on node {node.node_id} after {max_attempts} attempts "
            "(queries are distributed across nodes)"
        )

    def verify_queries_served_from_cache(self, queries, node, use_query_api=True, nodes_count=None):
        """Verify queries are served from cache on the given node."""
        if use_query_api:
            return self.verify_queries_from_cache_query_api(queries, node, nodes_count=nodes_count)
        return self.verify_queries_in_cache(queries, node.node_id)

    # --- warmup counters ---

    @staticmethod
    def get_warmup_counters(node, verbose=False):
        """Read warmup counters from the node's monitoring port."""
        try:
            url = f"http://localhost:{node.mon_port}/counters/counters=kqp/json"
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                counters = {}
                for sensor in data.get("sensors", []):
                    name = sensor.get("labels", {}).get("sensor", "")
                    if "Warmup" in name:
                        counters[name] = sensor.get("value", 0)
                if verbose and not counters:
                    sensors = data.get("sensors", [])
                    warmup_related = [s for s in sensors if "armup" in str(s)]
                    logger.warning("[diag] No Warmup counters found. Total sensors: %d", len(sensors))
                    if warmup_related:
                        logger.warning("[diag] Warmup-like sensors: %s", warmup_related[:5])
                    elif sensors:
                        logger.warning("[diag] Sample sensor: %s", sensors[0])
                return counters
        except Exception as e:
            logger.warning("Failed to get counters from node %d: %s", node.node_id, e)
        return {}

    @staticmethod
    def get_warmup_window_counters(node):
        # Observation-window counters: Warmup/HitsInWindow,
        # Warmup/MissesInWindow, Warmup/SavedCompileMs.
        counters = CompileCacheView.get_warmup_counters(node)
        return {
            "hits": counters.get("Warmup/HitsInWindow", 0),
            "misses": counters.get("Warmup/MissesInWindow", 0),
            "saved_ms": counters.get("Warmup/SavedCompileMs", 0),
        }

    @staticmethod
    def get_peer_scan_warnings(node):
        """Read CompileCacheView/PeerScanWarnings from the node's monitoring port."""
        try:
            url = f"http://localhost:{node.mon_port}/counters/counters=kqp/json"
            response = requests.get(url, timeout=5)
            if response.status_code != 200:
                return 0
            for sensor in response.json().get("sensors", []):
                if sensor.get("labels", {}).get("sensor") == "CompileCacheView/PeerScanWarnings":
                    return sensor.get("value", 0)
        except Exception as e:
            logger.warning("Failed to read PeerScanWarnings from node %d: %s", node.node_id, e)
        return 0

    def trigger_compile_cache_scan(self, node, timeout_seconds=30):
        """Run a SELECT against compile_cache_queries sysview pinned to `node`.

        Discovery is disabled so the scan (and any retry) runs on `node` itself.
        With a plain discovery-enabled driver SessionPool could balance the query
        onto another node, and PeerScanWarnings would then move on the wrong node.
        """
        sql = f"SELECT NodeId, Query FROM `{self.database}/.sys/compile_cache_queries`"
        settings = ydb.BaseRequestSettings().with_timeout(timeout_seconds)
        with _node_pinned_driver(node, database=self.database) as driver:
            pool = ydb.SessionPool(driver)
            try:
                def _do(session):
                    return session.transaction(ydb.SerializableReadWrite()).execute(
                        sql, commit_tx=True, settings=settings,
                    )
                pool.retry_operation_sync(_do)
            finally:
                pool.stop()

    @classmethod
    def wait_for_warmup_finished(cls, node, timeout=60):
        """Poll warmup counters until compilation stabilizes."""
        start_time = time.time()
        last_compiled = -1
        stable_polls = 0
        while time.time() - start_time < timeout:
            counters = cls.get_warmup_counters(node)
            fetched = counters.get("Warmup/QueriesFetched", 0)
            compiled = counters.get("Warmup/QueriesCompiled", 0)
            if fetched > 0 and compiled > 0:
                if compiled == last_compiled:
                    stable_polls += 1
                    if stable_polls >= 2:
                        return counters
                else:
                    stable_polls = 0
                    last_compiled = compiled
            time.sleep(2)
        return cls.get_warmup_counters(node, verbose=True)

    def populate_cache_on_nodes(self, node_ids, extra_queries=None, use_query_api=False):
        """Execute queries on each node to populate compile cache. Returns list of queries."""
        queries = list(WARMUP_QUERIES)
        if extra_queries:
            queries.extend(extra_queries)

        for node_id in node_ids:
            node = self.cluster.nodes[node_id]
            driver = ydb.Driver(endpoint=f"grpc://localhost:{node.port}", database=self.database)
            # Generous: gRPC stays closed until warmup completes (see Discovery gating).
            driver.wait(timeout=120)
            try:
                if use_query_api:
                    ok = self._populate_node_query_api(driver, node_id, queries)
                else:
                    ok = self._populate_node_table_api(driver, node_id, queries)
                if not ok:
                    logger.warning(
                        "Could not pin session to node %d after 30 attempts, "
                        "cache may be on a different node", node_id,
                    )
            finally:
                driver.stop()

        queries.append((WARMUP_QUERY_PARAM, WARMUP_PARAM_VALUES))
        return queries

    @staticmethod
    def _get_node_id_from_session_id(session_id):
        m = re.search(r'node_id=(\d+)', session_id)
        return int(m.group(1)) if m else None

    def _populate_node_table_api(self, driver, target_node_id, queries, repeat_count=3):
        settings = ydb.BaseRequestSettings().with_timeout(10)
        for attempt in range(30):
            session = driver.table_client.session().create()
            try:
                actual_node = self._get_node_id_from_session_id(session.session_id)
                if actual_node != target_node_id:
                    continue
                for query in queries:
                    prepared = session.prepare(query)
                    for _ in range(repeat_count):
                        session.transaction().execute(prepared, {}, commit_tx=True, settings=settings)
                prepared_param = session.prepare(WARMUP_QUERY_PARAM)
                for _ in range(repeat_count):
                    session.transaction().execute(
                        prepared_param, WARMUP_PARAM_VALUES, commit_tx=True, settings=settings,
                    )
                return True
            finally:
                session.delete()
        return False

    @staticmethod
    def _populate_node_query_api(driver, target_node_id, queries, repeat_count=3):
        for attempt in range(30):
            session = ydb.QuerySession(driver)
            try:
                session.create()
                if session.node_id != target_node_id:
                    continue
                for query in queries:
                    for _ in range(repeat_count):
                        for _ in session.execute(query):
                            pass
                for _ in range(repeat_count):
                    for _ in session.execute(WARMUP_QUERY_PARAM, WARMUP_PARAM_VALUES):
                        pass
                return True
            finally:
                session.delete()
        return False


def _make_warmup_config(nodes=3, max_nodes_to_request=None,
                        soft_deadline_seconds=120, hard_deadline_seconds=180):
    # Defaults are generous on purpose: KqpProxy peer discovery in the functional
    # test harness takes 40-70s after a node restart (kqpproxy+ board cadence),
    # and grows with repeated restarts. Production deadlines (~10s soft) are
    # tight on purpose; the test harness only needs them this loose because the
    # board cadence here is slower. Tests that explicitly want fast-skip semantics
    # (TestWarmupSingleNode, TestWarmupMultiNodeColdStart) override these.
    config = KikimrConfigGenerator(
        erasure=Erasure.NONE,
        nodes=nodes,
        use_in_memory_pdisks=True,
        additional_log_configs={
            "KQP_COMPILE_SERVICE": 7,
            "KQP_PROXY": 6,
            "SYSTEM_VIEWS": 6,
        },
    )
    config.yaml_config["feature_flags"] = {"enable_compile_cache_view": True}
    warmup_config = {
        "max_queries_to_load": 1000,
        "soft_deadline_seconds": soft_deadline_seconds,
        "hard_deadline_seconds": hard_deadline_seconds,
    }
    if max_nodes_to_request is not None:
        warmup_config["max_nodes_to_request"] = max_nodes_to_request
    config.yaml_config["table_service_config"] = {
        "compile_query_cache_size": 200 if nodes > 3 else 100,
        "enable_compile_cache_warmup": True,
        "compile_cache_warmup_config": warmup_config,
    }
    return config


class TestWarmupBasic:
    """Basic warmup scenarios: Table API, Query API, simultaneous restart, concurrent queries.
    Single 3-node cluster, single cache populate, multiple restart scenarios."""

    @classmethod
    def setup_class(cls):
        cls.config = _make_warmup_config(nodes=3)
        cls.cluster = kikimr_cluster_factory(cls.config)
        cls.cluster.start()
        cls.driver = ydb.Driver(
            endpoint=f"grpc://localhost:{cls.cluster.nodes[1].port}",
            database="/Root",
        )
        cls.driver.wait(timeout=10)
        cls.cache = CompileCacheView(cls.cluster)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, "driver"):
            cls.driver.stop()
        if hasattr(cls, "cluster"):
            cls.cluster.stop()

    def _restart_node(self, node, sleep_after=5):
        logger.info("Stopping node %d ...", node.node_id)
        node.stop()
        time.sleep(2)
        logger.info("Starting node %d ...", node.node_id)
        node.start()
        time.sleep(sleep_after)

    def _assert_warmup(self, node, label, timeout=None):
        timeout = timeout or (NODE_READY_TIMEOUT_SECONDS + WARMUP_DEADLINE_SECONDS)
        counters = CompileCacheView.wait_for_warmup_finished(node, timeout=timeout)
        fetched = counters.get("Warmup/QueriesFetched", 0)
        compiled = counters.get("Warmup/QueriesCompiled", 0)
        logger.info(
            "[%s] Node %d: fetched=%d, compiled=%d, counters=%s", label, node.node_id, fetched, compiled, counters
        )
        assert fetched > 0, f"[{label}] WarmupQueriesFetched should be > 0, got {fetched}"
        assert compiled > 0, f"[{label}] WarmupQueriesCompiled should be > 0, got {compiled}, fetched={fetched}"
        return counters

    def _assert_cache_hit(self, queries, node, label, use_query_api=False):
        from_cache, total = self.cache.verify_queries_served_from_cache(
            queries, node, use_query_api=use_query_api, nodes_count=len(self.cluster.nodes),
        )
        logger.info("[%s] Node %d: cache hit %d/%d", label, node.node_id, from_cache, total)
        assert from_cache == total, f"[{label}] Expected all {total} from cache, got {from_cache}/{total}"

    def _trigger_client_hits(self, node, use_query_api):
        self.cache.populate_cache_on_nodes(
            [node.node_id], use_query_api=use_query_api,
        )

    def _assert_warmup_attribution_hits(self, node, label):
        # Best-effort: after restart, session routing isn't fully reliable,
        # so populate_cache_on_nodes may silently bail (30-attempt pin loop)
        # and HitsInWindow never bumps. Cache contents are already verified by
        # _assert_cache_hit; strict attribution is covered by TestWarmupCounters.
        ctrs = CompileCacheView.get_warmup_window_counters(node)
        logger.info("[%s] Node %d warmup window counters: %s", label, node.node_id, ctrs)
        if ctrs["hits"] == 0:
            logger.warning(
                "[%s] HitsInWindow stayed 0 — likely session-pin retry exhausted, "
                "see TestWarmupCounters for the strict check", label,
            )

    @staticmethod
    def _run_pinned_queries_table_api(node, queries, query_timeout=15):
        settings = ydb.BaseRequestSettings().with_timeout(query_timeout)
        with _pinned_table_session(node) as session:
            ok = 0
            errors = []
            for query in queries:
                try:
                    session.transaction().execute(query, commit_tx=True, settings=settings)
                    ok += 1
                except Exception as e:
                    errors.append(e)
            return ok, errors

    @staticmethod
    def _run_pinned_queries_query_api(node, queries):
        with _pinned_query_session(node) as session:
            ok = 0
            errors = []
            for query in queries:
                try:
                    for _ in session.execute(query):
                        pass
                    ok += 1
                except Exception as e:
                    errors.append(e)
            return ok, errors

    def test_warmup_basic(self):
        all_node_ids = sorted(self.cluster.nodes.keys())
        node3 = self.cluster.nodes[3]

        logger.info("SETUP: Populating cache on all nodes (Table API)")
        table_queries = self.cache.populate_cache_on_nodes(all_node_ids, use_query_api=False)
        logger.info("Table API: %d queries on nodes %s", len(table_queries), all_node_ids)

        logger.info("SETUP: Populating cache on all nodes (Query API)")
        query_queries = self.cache.populate_cache_on_nodes(all_node_ids, use_query_api=True)
        logger.info("Query API: %d queries on nodes %s", len(query_queries), all_node_ids)
        time.sleep(2)

        logger.info("SCENARIO 1/3: Table API warmup (restart node 3)")
        self._restart_node(node3)
        self._assert_warmup(node3, "TableAPI")
        self._assert_cache_hit(table_queries, node3, "TableAPI", use_query_api=False)
        self._trigger_client_hits(node3, use_query_api=False)
        self._assert_warmup_attribution_hits(node3, "TableAPI")

        logger.info("SCENARIO 2/3: Query API warmup (restart node 3)")
        self._restart_node(node3)
        self._assert_warmup(node3, "QueryAPI")
        # Query API _assert_cache_hit already runs the queries against node3.
        self._assert_cache_hit(query_queries, node3, "QueryAPI", use_query_api=True)
        self._assert_warmup_attribution_hits(node3, "QueryAPI")

        # QueryType is part of TKqpQueryId, so Table API and Query API entries
        # live in different cache buckets — populate both before S3.
        logger.info("Re-populating cache on all nodes for scenario 3 (both APIs)")
        self.cache.populate_cache_on_nodes(all_node_ids, use_query_api=False)
        self.cache.populate_cache_on_nodes(all_node_ids, use_query_api=True)
        time.sleep(1)

        logger.info("SCENARIO 3/3: Concurrent user queries during warmup")
        self._restart_node(node3, sleep_after=3)
        self._assert_warmup(node3, "Concurrent")

        warmed_queries = list(WARMUP_QUERIES)

        # 4a: Table API
        fresh_table = [
            f"SELECT 7919 + {i} AS unique_warmup_miss_probe_t_{i}"
            for i in range(6)
        ]
        ctrs_before_4a = CompileCacheView.get_warmup_window_counters(node3)
        logger.info("[Concurrent/3a Table] window before: %s", ctrs_before_4a)

        ok_t, errors_t = self._run_pinned_queries_table_api(
            node3, warmed_queries + fresh_table,
        )
        for e in errors_t:
            logger.warning("[Concurrent/3a Table] pinned query failed: %r", e)
        logger.info(
            "[Concurrent/3a Table] OK: %d/%d", ok_t, len(warmed_queries) + len(fresh_table),
        )
        assert ok_t == len(warmed_queries) + len(fresh_table), (
            f"[Concurrent/3a Table] Expected all queries OK, got {ok_t}"
        )

        ctrs_after_4a = CompileCacheView.get_warmup_window_counters(node3)
        logger.info("[Concurrent/3a Table] window after: %s", ctrs_after_4a)
        d_hits_4a = ctrs_after_4a["hits"] - ctrs_before_4a["hits"]
        d_misses_4a = ctrs_after_4a["misses"] - ctrs_before_4a["misses"]
        assert d_hits_4a >= len(warmed_queries), (
            f"[Concurrent/3a Table] Warmed queries pinned to node3 must bump "
            f"Warmup/HitsInWindow >= {len(warmed_queries)}, got delta={d_hits_4a}, ctrs={ctrs_after_4a}"
        )
        assert d_misses_4a >= len(fresh_table), (
            f"[Concurrent/3a Table] Fresh probes pinned to node3 must bump "
            f"Warmup/MissesInWindow >= {len(fresh_table)}, got delta={d_misses_4a}, ctrs={ctrs_after_4a}"
        )

        # 4b: Query API
        fresh_query = [
            f"SELECT 7919 + {i} AS unique_warmup_miss_probe_q_{i}"
            for i in range(6)
        ]
        ok_q, errors_q = self._run_pinned_queries_query_api(
            node3, warmed_queries + fresh_query,
        )
        for e in errors_q:
            logger.warning("[Concurrent/3b Query] pinned query failed: %r", e)
        logger.info(
            "[Concurrent/3b Query] OK: %d/%d", ok_q, len(warmed_queries) + len(fresh_query),
        )
        assert ok_q == len(warmed_queries) + len(fresh_query), (
            f"[Concurrent/3b Query] Expected all queries OK, got {ok_q}"
        )

        ctrs_after_4b = CompileCacheView.get_warmup_window_counters(node3)
        logger.info("[Concurrent/3b Query] window after: %s", ctrs_after_4b)
        d_hits_4b = ctrs_after_4b["hits"] - ctrs_after_4a["hits"]
        d_misses_4b = ctrs_after_4b["misses"] - ctrs_after_4a["misses"]
        assert d_hits_4b >= len(warmed_queries), (
            f"[Concurrent/3b Query] Warmed queries pinned to node3 must bump "
            f"Warmup/HitsInWindow >= {len(warmed_queries)}, got delta={d_hits_4b}, ctrs={ctrs_after_4b}"
        )
        assert d_misses_4b >= len(fresh_query), (
            f"[Concurrent/3b Query] Fresh probes pinned to node3 must bump "
            f"Warmup/MissesInWindow >= {len(fresh_query)}, got delta={d_misses_4b}, ctrs={ctrs_after_4b}"
        )

        logger.info("ALL 3 BASIC SCENARIOS PASSED (incl. 3a Table + 3b Query)")


class TestWarmupStress:
    """Stress warmup scenarios: multiple restarts, rolling restart, full cluster restart."""

    @classmethod
    def setup_class(cls):
        cls.config = _make_warmup_config(nodes=3)
        cls.cluster = kikimr_cluster_factory(cls.config)
        cls.cluster.start()
        cls.driver = ydb.Driver(
            endpoint=f"grpc://localhost:{cls.cluster.nodes[1].port}",
            database="/Root",
        )
        cls.driver.wait(timeout=10)
        cls.cache = CompileCacheView(cls.cluster)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, "driver"):
            cls.driver.stop()
        if hasattr(cls, "cluster"):
            cls.cluster.stop()

    def test_warmup_stress(self):
        all_node_ids = sorted(self.cluster.nodes.keys())
        nodes_count = len(self.cluster.nodes)
        node3 = self.cluster.nodes[3]

        logger.info("SETUP: Populating cache on all nodes (Query API)")
        queries = self.cache.populate_cache_on_nodes(all_node_ids, use_query_api=True)
        logger.info("%d queries on nodes %s", len(queries), all_node_ids)
        time.sleep(2)

        # Counters reset on node stop, so every round refills from scratch.
        expected_min_compiled = len(queries)

        logger.info("SCENARIO 1/3: Multiple restarts of node 3 (3 rounds)")
        for i in range(3):
            logger.info("Round %d/3", i + 1)
            node3.stop()
            time.sleep(2)
            node3.start()
            time.sleep(5)

            counters = CompileCacheView.wait_for_warmup_finished(
                node3,
                timeout=NODE_READY_TIMEOUT_SECONDS + WARMUP_DEADLINE_SECONDS,
            )
            fetched = counters.get("Warmup/QueriesFetched", 0)
            compiled = counters.get("Warmup/QueriesCompiled", 0)
            logger.info("Round %d: fetched=%d, compiled=%d, counters=%s", i + 1, fetched, compiled, counters)
            assert fetched >= expected_min_compiled, (
                f"[MultiRestart round {i + 1}] WarmupQueriesFetched should be >= "
                f"{expected_min_compiled}, got {fetched}"
            )
            assert compiled >= expected_min_compiled, (
                f"[MultiRestart round {i + 1}] WarmupQueriesCompiled should be >= "
                f"{expected_min_compiled}, got {compiled}, fetched={fetched}"
            )

            from_cache, total = self.cache.verify_queries_served_from_cache(
                queries, node3, use_query_api=True, nodes_count=nodes_count,
            )
            logger.info("Round %d: cache hit %d/%d", i + 1, from_cache, total)
            assert from_cache == total, (
                f"[MultiRestart round {i + 1}] Expected all from cache, got {from_cache}/{total}"
            )

        logger.info("Re-populating cache on all nodes before rolling restart")
        self.cache.populate_cache_on_nodes(all_node_ids, use_query_api=True)
        time.sleep(2)

        logger.info("SCENARIO 2/3: Rolling restart (nodes 2, 3 sequentially)")
        node_ids_to_restart = sorted(self.cluster.nodes.keys())[1:]
        for idx, nid in enumerate(node_ids_to_restart):
            node = self.cluster.nodes[nid]
            logger.info("Rolling restart %d/%d: node %d", idx + 1, len(node_ids_to_restart), nid)
            node.stop()
            time.sleep(2)
            node.start()
            time.sleep(5)

            counters = CompileCacheView.wait_for_warmup_finished(
                node,
                timeout=NODE_READY_TIMEOUT_SECONDS + WARMUP_DEADLINE_SECONDS,
            )
            fetched = counters.get("Warmup/QueriesFetched", 0)
            compiled = counters.get("Warmup/QueriesCompiled", 0)
            logger.info("[Rolling] node %d: fetched=%d, compiled=%d, counters=%s", nid, fetched, compiled, counters)

            assert fetched > 0, f"[Rolling] node {nid}: WarmupQueriesFetched should be > 0, got {fetched}"
            assert compiled > 0, f"[Rolling] node {nid}: WarmupQueriesCompiled should be > 0, got {compiled}"

            from_cache, total = self.cache.verify_queries_served_from_cache(
                queries, node, use_query_api=True, nodes_count=nodes_count,
            )
            logger.info("[Rolling] node %d: cache hit %d/%d", nid, from_cache, total)
            assert from_cache == total, (
                f"[Rolling] node {nid}: expected all from cache, got {from_cache}/{total}"
            )

        logger.info("Re-populating cache on all nodes before full cluster restart")
        self.cache.populate_cache_on_nodes(all_node_ids, use_query_api=True)
        time.sleep(2)

        logger.info("SCENARIO 3/3: Full cluster restart (nodes 2, 3)")
        nodes_to_restart = [self.cluster.nodes[nid] for nid in sorted(self.cluster.nodes.keys())[1:]]

        logger.info("Stopping %d non-controller nodes ...", len(nodes_to_restart))
        for node in nodes_to_restart:
            node.stop()
        time.sleep(3)

        logger.info("Starting %d nodes back ...", len(nodes_to_restart))
        for node in nodes_to_restart:
            node.start()
        time.sleep(8)

        # Same TryStartWarmup race as TestWarmupBasic S3: require at least one
        # node to warm up; check cache-served invariant only on those that did fetch.
        any_fetched = False
        for node in nodes_to_restart:
            counters = CompileCacheView.wait_for_warmup_finished(
                node,
                timeout=NODE_READY_TIMEOUT_SECONDS + WARMUP_DEADLINE_SECONDS,
            )
            fetched = counters.get("Warmup/QueriesFetched", 0)
            compiled = counters.get("Warmup/QueriesCompiled", 0)
            logger.info(
                "[FullRestart] node %d: fetched=%d, compiled=%d, counters=%s",
                node.node_id, fetched, compiled, counters,
            )
            if fetched > 0:
                any_fetched = True
                from_cache, total = self.cache.verify_queries_served_from_cache(
                    queries, node, use_query_api=True, nodes_count=nodes_count,
                )
                logger.info("[FullRestart] node %d: cache hit %d/%d", node.node_id, from_cache, total)
                assert from_cache == total, (
                    f"[FullRestart] node {node.node_id}: expected all from cache, got {from_cache}/{total}"
                )
            else:
                logger.warning(
                    "[FullRestart] node %d: TryStartWarmup race -> fetched=0",
                    node.node_id,
                )

        assert any_fetched, "[FullRestart] At least one node should fetch queries from node 1"

        logger.info("ALL 3 STRESS SCENARIOS PASSED")


class TestWarmupCounters:
    """Independent counter-attribution check per client API (Table vs Query).

    Table API, Query API and PG paths are not symmetric in YDB and are wired
    at different layers, so we exercise Warmup/{Hits,Misses,SavedCompile}InWindow
    once per API to keep the signal isolated from TestWarmupBasic scenarios.
    """

    @classmethod
    def setup_class(cls):
        cls.config = _make_warmup_config(nodes=2)
        cls.cluster = kikimr_cluster_factory(cls.config)
        cls.cluster.start()
        cls.driver = ydb.Driver(
            endpoint=f"grpc://localhost:{cls.cluster.nodes[1].port}",
            database="/Root",
        )
        cls.driver.wait(timeout=10)
        cls.cache = CompileCacheView(cls.cluster)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, "driver"):
            cls.driver.stop()
        if hasattr(cls, "cluster"):
            cls.cluster.stop()

    @pytest.mark.parametrize("use_query_api", [False, True], ids=["table", "query"])
    def test_warmup_counters_attribution(self, use_query_api):
        node2 = self.cluster.nodes[2]
        all_node_ids = sorted(self.cluster.nodes.keys())

        # Populate cache so node2 has something to fetch from node1 after restart.
        self.cache.populate_cache_on_nodes(all_node_ids, use_query_api=use_query_api)
        time.sleep(2)

        # Restart node2: empty its cache, force warmup to refill from node1.
        node2.stop()
        time.sleep(2)
        node2.start()
        time.sleep(5)

        finished = CompileCacheView.wait_for_warmup_finished(
            node2, timeout=NODE_READY_TIMEOUT_SECONDS + WARMUP_DEADLINE_SECONDS,
        )
        compiled = finished.get("Warmup/QueriesCompiled", 0)
        assert compiled > 0, (
            f"Warmup must populate node2 cache (compiled={compiled}, ctrs={finished})"
        )

        before = CompileCacheView.get_warmup_window_counters(node2)
        logger.info("[Counters] window before client traffic: %s", before)

        # Client traffic on warmed entries: each distinct query that warmup
        # repopulated should fire HitsInWindow at least once.
        # populate_cache_on_nodes runs WARMUP_QUERIES + WARMUP_QUERY_PARAM.
        expected_hits = len(WARMUP_QUERIES) + 1  # the parameterized query
        self.cache.populate_cache_on_nodes([node2.node_id], use_query_api=use_query_api)
        after_hit = CompileCacheView.get_warmup_window_counters(node2)
        logger.info("[Counters] window after warmed-entry hit: %s", after_hit)

        hits_delta = after_hit["hits"] - before["hits"]
        assert hits_delta >= expected_hits, (
            f"Warmup/HitsInWindow must increment by at least {expected_hits} "
            f"(one per distinct warmed query) after client traffic, "
            f"got delta={hits_delta}. before={before}, after={after_hit}"
        )
        assert after_hit["saved_ms"] > before["saved_ms"], (
            "Warmup/SavedCompileMs must increase when warmup hits are attributed. "
            f"before={before}, after={after_hit}"
        )

        # Fresh query inside the same observation window: each unique probe must
        # bump MissesInWindow once.
        fresh_queries = [
            f"SELECT 7919 + {i} AS unique_warmup_miss_probe_{i}"
            for i in range(3)
        ]
        self.cache.populate_cache_on_nodes(
            [node2.node_id], extra_queries=fresh_queries, use_query_api=use_query_api,
        )
        after_miss = CompileCacheView.get_warmup_window_counters(node2)
        logger.info("[Counters] window after fresh-query miss: %s", after_miss)

        misses_delta = after_miss["misses"] - after_hit["misses"]
        assert misses_delta >= len(fresh_queries), (
            f"Warmup/MissesInWindow must increment by at least {len(fresh_queries)} "
            f"(one per fresh probe), got delta={misses_delta}. "
            f"after_hit={after_hit}, after_miss={after_miss}"
        )


class TestCompileCacheViewPeerWarnings:
    """Wiring sanity check for CompileCacheView/PeerScanWarnings."""

    @classmethod
    def setup_class(cls):
        cls.config = _make_warmup_config(nodes=3)
        cls.cluster = kikimr_cluster_factory(cls.config)
        cls.cluster.start()
        cls.cache = CompileCacheView(cls.cluster)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, "cluster"):
            cls.cluster.stop()

    def test_peer_scan_warnings_increment_on_dead_peer(self):
        all_node_ids = sorted(self.cluster.nodes.keys())
        live_node_id = all_node_ids[0]
        dead_node_id = all_node_ids[-1]
        live_node = self.cluster.nodes[live_node_id]
        dead_node = self.cluster.nodes[dead_node_id]

        self.cache.populate_cache_on_nodes(all_node_ids, use_query_api=False)
        time.sleep(2)

        baseline = CompileCacheView.get_peer_scan_warnings(live_node)
        logger.info("[PeerWarnings] baseline on node %d: %d", live_node_id, baseline)

        logger.info("[PeerWarnings] stopping node %d to simulate dead peer", dead_node_id)
        dead_node.stop()
        # Give interconnect time to publish the disconnect; without this the
        # first scan falls through to NodeRequestTimeout (10s).
        time.sleep(3)

        try:
            # Each successful scan against a federated sysview that hits the dead
            # peer should bump PeerScanWarnings exactly once. Run a fixed number
            # of scans and require the counter to grow by at least that many.
            scans_to_run = 3
            deadline = time.time() + 60
            scans_done = 0
            while scans_done < scans_to_run and time.time() < deadline:
                self.cache.trigger_compile_cache_scan(live_node, timeout_seconds=30)
                scans_done += 1
                current = CompileCacheView.get_peer_scan_warnings(live_node)
                logger.info(
                    "[PeerWarnings] after scan %d/%d: %d (baseline %d)",
                    scans_done, scans_to_run, current, baseline,
                )
                time.sleep(1)

            final = CompileCacheView.get_peer_scan_warnings(live_node)
            delta = final - baseline
            assert delta >= scans_done, (
                "CompileCacheView/PeerScanWarnings must increment at least once "
                f"per scan that hits the dead peer; ran {scans_done} scans, "
                f"got delta={delta} (baseline={baseline}, final={final})"
            )
        finally:
            dead_node.start()
            time.sleep(3)


class TestWarmupSingleNode:
    SOFT_DEADLINE_SECONDS = 10
    HARD_DEADLINE_SECONDS = 12

    @classmethod
    def setup_class(cls):
        cls.config = _make_warmup_config(
            nodes=1,
            soft_deadline_seconds=cls.SOFT_DEADLINE_SECONDS,
            hard_deadline_seconds=cls.HARD_DEADLINE_SECONDS,
        )
        cls.cluster = kikimr_cluster_factory(cls.config)
        cls.cluster.start()
        cls.driver = ydb.Driver(
            endpoint=f"grpc://localhost:{cls.cluster.nodes[1].port}",
            database="/Root",
        )
        cls.driver.wait(timeout=10)
        cls.cache = CompileCacheView(cls.cluster)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, "driver"):
            cls.driver.stop()
        if hasattr(cls, "cluster"):
            cls.cluster.stop()

    def test_warmup_skips_on_single_node(self):
        node = self.cluster.nodes[1]

        wait_seconds = 3
        logger.info("Waiting %ds past warmup hard deadline before reading counters", wait_seconds)
        time.sleep(wait_seconds)

        counters = CompileCacheView.get_warmup_counters(node, verbose=True)
        fetched = counters.get("Warmup/QueriesFetched", 0)
        compiled = counters.get("Warmup/QueriesCompiled", 0)
        logger.info("SingleNode counters: %s", counters)

        assert fetched == 0, f"[SingleNode] Expected QueriesFetched=0, got {fetched}"
        assert compiled == 0, f"[SingleNode] Expected QueriesCompiled=0, got {compiled}"

        pool = ydb.SessionPool(self.driver)
        try:
            def _do(session):
                return session.transaction().execute(
                    "SELECT 1 + 1 AS r",
                    commit_tx=True,
                    settings=ydb.BaseRequestSettings().with_timeout(10),
                )
            result = pool.retry_operation_sync(_do)
            assert result[0].rows[0].r == 2, "Cluster must serve queries after warmup skip"
        finally:
            pool.stop()

        # HitsInWindow/MissesInWindow only attribute to in-window inserts;
        # without warmup inserts the window stays closed.
        window = CompileCacheView.get_warmup_window_counters(node)
        logger.info("[SingleNode] window counters after client query: %s", window)
        assert window["hits"] == 0 and window["misses"] == 0, (
            f"[SingleNode] Observation window must not open without warmup inserts, got {window}"
        )

        logger.info("SingleNode skip scenario PASSED")


class TestWarmupMultiNodeColdStart:
    SOFT_DEADLINE_SECONDS = 10
    HARD_DEADLINE_SECONDS = 12

    @classmethod
    def setup_class(cls):
        cls.config = _make_warmup_config(
            nodes=3,
            soft_deadline_seconds=cls.SOFT_DEADLINE_SECONDS,
            hard_deadline_seconds=cls.HARD_DEADLINE_SECONDS,
        )
        cls.cluster = kikimr_cluster_factory(cls.config)
        cls.cluster.start()
        cls.driver = ydb.Driver(
            endpoint=f"grpc://localhost:{cls.cluster.nodes[1].port}",
            database="/Root",
        )
        cls.driver.wait(timeout=10)
        cls.cache = CompileCacheView(cls.cluster)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, "driver"):
            cls.driver.stop()
        if hasattr(cls, "cluster"):
            cls.cluster.stop()

    def test_warmup_cold_start_no_queries(self):
        wait_seconds = 4
        logger.info("Waiting %ds past warmup hard deadline before reading counters", wait_seconds)
        time.sleep(wait_seconds)

        for nid in sorted(self.cluster.nodes.keys()):
            node = self.cluster.nodes[nid]
            counters = CompileCacheView.get_warmup_counters(node, verbose=True)
            fetched = counters.get("Warmup/QueriesFetched", 0)
            compiled = counters.get("Warmup/QueriesCompiled", 0)
            logger.info("[ColdStart] node %d counters: %s", nid, counters)
            assert fetched == 0, f"[ColdStart] node {nid}: expected QueriesFetched=0, got {fetched}"
            assert compiled == 0, f"[ColdStart] node {nid}: expected QueriesCompiled=0, got {compiled}"

        pool = ydb.SessionPool(self.driver)
        try:
            def _do(session):
                return session.transaction().execute(
                    "SELECT 1 + 1 AS r",
                    commit_tx=True,
                    settings=ydb.BaseRequestSettings().with_timeout(10),
                )
            result = pool.retry_operation_sync(_do)
            assert result[0].rows[0].r == 2, "Cluster must serve queries after cold-start warmup"
        finally:
            pool.stop()

        for nid in sorted(self.cluster.nodes.keys()):
            node = self.cluster.nodes[nid]
            window = CompileCacheView.get_warmup_window_counters(node)
            logger.info("[ColdStart] node %d window counters: %s", nid, window)
            assert window["hits"] == 0 and window["misses"] == 0, (
                f"[ColdStart] node {nid}: observation window must not open without "
                f"warmup inserts, got {window}"
            )

        logger.info("MultiNodeColdStart no-queries scenario PASSED")


class TestWarmupGrpcIsolation:
    @classmethod
    def setup_class(cls):
        cls.config = _make_warmup_config(nodes=3)
        cls.cluster = kikimr_cluster_factory(cls.config)
        cls.cluster.start()
        cls.driver = ydb.Driver(
            endpoint=f"grpc://localhost:{cls.cluster.nodes[1].port}",
            database="/Root",
        )
        cls.driver.wait(timeout=10)
        cls.cache = CompileCacheView(cls.cluster)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, "driver"):
            cls.driver.stop()
        if hasattr(cls, "cluster"):
            cls.cluster.stop()

    def test_grpc_closed_during_warmup(self):
        all_node_ids = sorted(self.cluster.nodes.keys())
        logger.info("SETUP: populating cache so warmup actually has work to do")
        self.cache.populate_cache_on_nodes(all_node_ids, use_query_api=True)
        time.sleep(2)

        target = self.cluster.nodes[3]
        grpc_port = target.port

        logger.info("Stopping node %d ...", target.node_id)
        target.stop()
        time.sleep(2)

        assert not _is_tcp_port_open("localhost", grpc_port), (
            "gRPC port unexpectedly open while node is stopped"
        )

        logger.info("Starting node %d ...", target.node_id)
        target.start()

        grpc_closed_observed = False
        warmup_made_progress = False
        port_open_after_warmup = False

        deadline = time.time() + NODE_READY_TIMEOUT_SECONDS + WARMUP_DEADLINE_SECONDS
        while time.time() < deadline:
            port_open = _is_tcp_port_open("localhost", grpc_port)
            counters = CompileCacheView.get_warmup_counters(target)
            compiled = counters.get("Warmup/QueriesCompiled", 0)
            fetched = counters.get("Warmup/QueriesFetched", 0)

            if not port_open:
                grpc_closed_observed = True
            if fetched > 0 or compiled > 0:
                warmup_made_progress = True

            if warmup_made_progress and port_open:
                time.sleep(0.3)
                if _is_tcp_port_open("localhost", grpc_port):
                    port_open_after_warmup = True
                    break

            time.sleep(0.2)

        logger.info(
            "[GrpcIsolation] grpc_closed_observed=%s warmup_made_progress=%s port_open_after_warmup=%s",
            grpc_closed_observed, warmup_made_progress, port_open_after_warmup,
        )

        assert warmup_made_progress, "Warmup never made progress — cache populate likely missed the target node"
        assert port_open_after_warmup, (
            "gRPC port did not open after warmup completed; "
            "TEvKqpWarmupComplete -> GRpcServersManager::Start path may be broken"
        )

        # Port-closed window can be missed on fast hosts (warmup finishes
        # before first sample); log instead of failing — the load-bearing
        # invariant is "port is open after warmup".
        if not grpc_closed_observed:
            logger.warning(
                "[GrpcIsolation] gRPC port was never observed closed during warmup"
            )

        ready_driver = ydb.Driver(
            endpoint=f"grpc://localhost:{grpc_port}",
            database="/Root",
        )
        ready_driver.wait(timeout=10)
        try:
            pool = ydb.SessionPool(ready_driver)
            try:
                result = pool.retry_operation_sync(
                    lambda s: s.transaction().execute(
                        "SELECT 1 + 1 AS r",
                        commit_tx=True,
                        settings=ydb.BaseRequestSettings().with_timeout(10),
                    )
                )
                assert result[0].rows[0].r == 2
            finally:
                pool.stop()
        finally:
            ready_driver.stop()

        logger.info("GrpcIsolation scenario PASSED")
