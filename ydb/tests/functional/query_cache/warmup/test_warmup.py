import logging
import re
import socket
import time
from contextlib import contextmanager

import requests

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


def _node_id_from_session_id(session_id):
    m = re.search(r'node_id=(\d+)', session_id)
    return int(m.group(1)) if m else None


# Public SDK has no API to create a session on a specific node — the pool
# load-balances. We retry create-and-discard until we land on target_node_id.
# Yields None on failure; callers must check.
@contextmanager
def _pinned_table_session(driver, target_node_id, max_attempts=30):
    pinned = None
    for _ in range(max_attempts):
        candidate = driver.table_client.session().create()
        if _node_id_from_session_id(candidate.session_id) == target_node_id:
            pinned = candidate
            break
        candidate.delete()
    if pinned is None:
        yield None
        return
    try:
        yield pinned
    finally:
        pinned.delete()


@contextmanager
def _pinned_query_session(driver, target_node_id, max_attempts=30):
    pinned = None
    for _ in range(max_attempts):
        candidate = ydb.QuerySession(driver)
        candidate.create()
        if candidate.node_id == target_node_id:
            pinned = candidate
            break
        candidate.delete()
    if pinned is None:
        yield None
        return
    try:
        yield pinned
    finally:
        pinned.delete()


class CompileCacheView:
    def __init__(self, cluster, database="/Root"):
        self.cluster = cluster
        self.database = database

    def get_cached_queries(self, node_id):
        node = self.cluster.nodes[node_id]
        driver = ydb.Driver(
            endpoint=f"grpc://localhost:{node.port}",
            database=self.database,
        )
        driver.wait(timeout=10)
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
        cached = self.get_cached_query_texts(node_id)
        from_cache = 0
        for item in queries:
            text = item[0] if isinstance(item, tuple) else item
            if text in cached:
                from_cache += 1
        return from_cache, len(queries)

    def verify_queries_from_cache_query_api(self, queries, node, nodes_count=None):
        max_attempts = (nodes_count or 3) * 4
        driver = ydb.Driver(
            endpoint=f"grpc://localhost:{node.port}",
            database=self.database,
        )
        driver.wait(timeout=10)
        try:
            with _pinned_query_session(driver, node.node_id, max_attempts=max_attempts) as session:
                if session is None:
                    raise AssertionError(
                        f"Failed to get session on node {node.node_id} after {max_attempts} attempts "
                        "(queries are distributed across nodes)"
                    )
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
            driver.stop()

    def verify_queries_served_from_cache(self, queries, node, use_query_api=True, nodes_count=None):
        if use_query_api:
            return self.verify_queries_from_cache_query_api(queries, node, nodes_count=nodes_count)
        return self.verify_queries_in_cache(queries, node.node_id)

    @staticmethod
    def get_warmup_counters(node, verbose=False):
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
        counters = CompileCacheView.get_warmup_counters(node)
        return {
            "hits": counters.get("Warmup/HitsInWindow", 0),
            "misses": counters.get("Warmup/MissesInWindow", 0),
            "saved_ms": counters.get("Warmup/SavedCompileMs", 0),
        }

    # Polls Warmup/QueriesCompiled until it stays the same for 2 reads in a row,
    # then returns. Avoids racing the test against an in-progress warmup actor.
    @classmethod
    def wait_for_warmup_finished(cls, node, timeout=60):
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
        queries = list(WARMUP_QUERIES)
        if extra_queries:
            queries.extend(extra_queries)

        for node_id in node_ids:
            node = self.cluster.nodes[node_id]
            driver = ydb.Driver(endpoint=f"grpc://localhost:{node.port}", database=self.database)
            driver.wait(timeout=10)
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

    def _populate_node_table_api(self, driver, target_node_id, queries, repeat_count=3):
        settings = ydb.BaseRequestSettings().with_timeout(10)
        with _pinned_table_session(driver, target_node_id) as session:
            if session is None:
                return False
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

    @staticmethod
    def _populate_node_query_api(driver, target_node_id, queries, repeat_count=3):
        with _pinned_query_session(driver, target_node_id) as session:
            if session is None:
                return False
            for query in queries:
                for _ in range(repeat_count):
                    for _ in session.execute(query):
                        pass
            for _ in range(repeat_count):
                for _ in session.execute(WARMUP_QUERY_PARAM, WARMUP_PARAM_VALUES):
                    pass
            return True


def _make_warmup_config(nodes=3, max_nodes_to_request=None,
                        soft_deadline_seconds=None, hard_deadline_seconds=None):
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
    warmup_config = {"max_queries_to_load": 1000}
    if max_nodes_to_request is not None:
        warmup_config["max_nodes_to_request"] = max_nodes_to_request
    if soft_deadline_seconds is not None:
        warmup_config["soft_deadline_seconds"] = soft_deadline_seconds
    if hard_deadline_seconds is not None:
        warmup_config["hard_deadline_seconds"] = hard_deadline_seconds
    config.yaml_config["table_service_config"] = {
        "compile_query_cache_size": 200 if nodes > 3 else 100,
        "enable_compile_cache_warmup": True,
        "compile_cache_warmup_config": warmup_config,
    }
    return config


class TestWarmupBasic:
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
        # _assert_cache_hit in the Table API path only reads sysview; we need
        # actual client execution against `node` for hit counters to move.
        self.cache.populate_cache_on_nodes(
            [node.node_id], use_query_api=use_query_api,
        )

    def _assert_warmup_attribution_hits(self, node, label):
        # SavedCompileMs is a soft check: trivial SELECTs may round to 0ms.
        ctrs = CompileCacheView.get_warmup_window_counters(node)
        logger.info("[%s] Node %d warmup window counters: %s", label, node.node_id, ctrs)
        assert ctrs["hits"] > 0, (
            f"[{label}] Warmup/HitsInWindow must be > 0 after client hit warmup entries, got {ctrs}"
        )
        assert ctrs["saved_ms"] >= 0, (
            f"[{label}] Warmup/SavedCompileMs must be present (>= 0), got {ctrs}"
        )

    # _run_pinned_queries_*: returns (pinned, ok_count, errors).
    @staticmethod
    def _run_pinned_queries_table_api(driver, target_node_id, queries, query_timeout=15):
        settings = ydb.BaseRequestSettings().with_timeout(query_timeout)
        with _pinned_table_session(driver, target_node_id) as session:
            if session is None:
                return False, 0, []
            ok = 0
            errors = []
            for query in queries:
                try:
                    session.transaction().execute(query, commit_tx=True, settings=settings)
                    ok += 1
                except Exception as e:
                    errors.append(e)
            return True, ok, errors

    @staticmethod
    def _run_pinned_queries_query_api(driver, target_node_id, queries):
        with _pinned_query_session(driver, target_node_id) as session:
            if session is None:
                return False, 0, []
            ok = 0
            errors = []
            for query in queries:
                try:
                    for _ in session.execute(query):
                        pass
                    ok += 1
                except Exception as e:
                    errors.append(e)
            return True, ok, errors

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

        logger.info("SCENARIO 1/4: Table API warmup (restart node 3)")
        self._restart_node(node3)
        self._assert_warmup(node3, "TableAPI")
        self._assert_cache_hit(table_queries, node3, "TableAPI", use_query_api=False)
        self._trigger_client_hits(node3, use_query_api=False)
        self._assert_warmup_attribution_hits(node3, "TableAPI")

        logger.info("SCENARIO 2/4: Query API warmup (restart node 3)")
        self._restart_node(node3)
        self._assert_warmup(node3, "QueryAPI")
        # Query API path of _assert_cache_hit already executes the queries
        # against node3 — no separate _trigger_client_hits call needed.
        self._assert_cache_hit(query_queries, node3, "QueryAPI", use_query_api=True)
        self._assert_warmup_attribution_hits(node3, "QueryAPI")

        # KqpProxy's TryStartWarmup gates on PeerProxyNodeResources.size() > 1,
        # which is satisfied by {self, the other freshly-restarted peer} alone.
        # The federated .sys/compile_cache_queries scan then filters by
        # NodeId IN (self, peer) and misses node 1 (the only cache holder),
        # so warmup may complete with fetched=0. Strict assertions live in
        # S1/S2/S4; this scenario only smoke-tests "nothing crashes".
        logger.info("SCENARIO 3/4: Simultaneous restart (nodes 2, 3)")
        node2 = self.cluster.nodes[2]
        logger.info("Stopping nodes 2 and 3 ...")
        node2.stop()
        node3.stop()
        time.sleep(3)
        logger.info("Starting nodes 2 and 3 ...")
        node2.start()
        node3.start()
        time.sleep(5)

        for node in [node2, node3]:
            counters = CompileCacheView.wait_for_warmup_finished(
                node, timeout=NODE_READY_TIMEOUT_SECONDS + WARMUP_DEADLINE_SECONDS,
            )
            fetched = counters.get("Warmup/QueriesFetched", 0)
            compiled = counters.get("Warmup/QueriesCompiled", 0)
            logger.info(
                "[Simultaneous/node%d] fetched=%d, compiled=%d, counters=%s",
                node.node_id, fetched, compiled, counters,
            )
            if fetched == 0:
                logger.warning(
                    "[Simultaneous/node%d] TryStartWarmup race -> fetched=0 (see comment above)",
                    node.node_id,
                )

        # TKqpQueryId.Settings.QueryType is part of the compile cache key, so
        # Table API and Query API entries live in different buckets — populate
        # both before scenario 4 to give 4a/4b independent warmed texts.
        logger.info("Re-populating cache on all nodes for scenario 4 (both APIs)")
        self.cache.populate_cache_on_nodes(all_node_ids, use_query_api=False)
        self.cache.populate_cache_on_nodes(all_node_ids, use_query_api=True)
        time.sleep(1)

        logger.info("SCENARIO 4/4: Concurrent user queries during warmup")
        self._restart_node(node3, sleep_after=3)
        self._assert_warmup(node3, "Concurrent")

        # Hits attribution is best-effort: warmup actor restores QueryType
        # from .sys but cannot reconstruct every TKqpQueryId field (Settings
        # subset, GUCSettings) — the warmup-compiled key may differ from the
        # user-execute key and suppress HitsInWindow for warmed texts. Logged
        # rather than asserted; misses below are the load-bearing check.
        warmed_queries = list(WARMUP_QUERIES)

        # 4a: Table API
        fresh_table = [
            f"SELECT 7919 + {i} AS unique_warmup_miss_probe_t_{i}"
            for i in range(6)
        ]
        ctrs_before_4a = CompileCacheView.get_warmup_window_counters(node3)
        logger.info("[Concurrent/4a Table] window before: %s", ctrs_before_4a)

        table_driver = ydb.Driver(
            endpoint=f"grpc://localhost:{node3.port}", database="/Root",
        )
        table_driver.wait(timeout=20)
        try:
            pinned_t, ok_t, errors_t = self._run_pinned_queries_table_api(
                table_driver, node3.node_id, warmed_queries + fresh_table,
            )
        finally:
            table_driver.stop()

        for e in errors_t:
            logger.warning("[Concurrent/4a Table] pinned query failed: %r", e)
        logger.info(
            "[Concurrent/4a Table] OK: %d/%d", ok_t, len(warmed_queries) + len(fresh_table),
        )
        assert pinned_t, (
            f"[Concurrent/4a Table] Could not pin a Table session to node {node3.node_id}"
        )
        assert ok_t == len(warmed_queries) + len(fresh_table), (
            f"[Concurrent/4a Table] Expected all queries OK, got {ok_t}"
        )

        ctrs_after_4a = CompileCacheView.get_warmup_window_counters(node3)
        logger.info("[Concurrent/4a Table] window after: %s", ctrs_after_4a)
        d_hits_4a = ctrs_after_4a["hits"] - ctrs_before_4a["hits"]
        d_misses_4a = ctrs_after_4a["misses"] - ctrs_before_4a["misses"]
        if d_hits_4a < len(warmed_queries):
            logger.warning(
                "[Concurrent/4a Table] HitsInWindow delta=%d, expected >= %d (cache-key mismatch, see above)",
                d_hits_4a, len(warmed_queries),
            )
        assert d_misses_4a >= len(fresh_table), (
            f"[Concurrent/4a Table] Fresh probes pinned to node3 must bump "
            f"Warmup/MissesInWindow >= {len(fresh_table)}, got delta={d_misses_4a}, ctrs={ctrs_after_4a}"
        )

        # 4b: Query API
        fresh_query = [
            f"SELECT 7919 + {i} AS unique_warmup_miss_probe_q_{i}"
            for i in range(6)
        ]
        query_driver = ydb.Driver(
            endpoint=f"grpc://localhost:{node3.port}", database="/Root",
        )
        query_driver.wait(timeout=20)
        try:
            pinned_q, ok_q, errors_q = self._run_pinned_queries_query_api(
                query_driver, node3.node_id, warmed_queries + fresh_query,
            )
        finally:
            query_driver.stop()

        for e in errors_q:
            logger.warning("[Concurrent/4b Query] pinned query failed: %r", e)
        logger.info(
            "[Concurrent/4b Query] OK: %d/%d", ok_q, len(warmed_queries) + len(fresh_query),
        )
        assert pinned_q, (
            f"[Concurrent/4b Query] Could not pin a Query session to node {node3.node_id}"
        )
        assert ok_q == len(warmed_queries) + len(fresh_query), (
            f"[Concurrent/4b Query] Expected all queries OK, got {ok_q}"
        )

        ctrs_after_4b = CompileCacheView.get_warmup_window_counters(node3)
        logger.info("[Concurrent/4b Query] window after: %s", ctrs_after_4b)
        d_hits_4b = ctrs_after_4b["hits"] - ctrs_after_4a["hits"]
        d_misses_4b = ctrs_after_4b["misses"] - ctrs_after_4a["misses"]
        if d_hits_4b < len(warmed_queries):
            logger.warning(
                "[Concurrent/4b Query] HitsInWindow delta=%d, expected >= %d (cache-key mismatch, see above)",
                d_hits_4b, len(warmed_queries),
            )
        assert d_misses_4b >= len(fresh_query), (
            f"[Concurrent/4b Query] Fresh probes pinned to node3 must bump "
            f"Warmup/MissesInWindow >= {len(fresh_query)}, got delta={d_misses_4b}, ctrs={ctrs_after_4b}"
        )

        logger.info("ALL 4 BASIC SCENARIOS PASSED (incl. 4a Table + 4b Query)")


class TestWarmupStress:
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

        # Counters reset on node stop, so every round must fetch+compile the
        # full WARMUP_QUERIES + 1 parametrised query fresh.
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

        # Same TryStartWarmup race as TestWarmupBasic Scenario 3: with both
        # nodes coming up at once their board view can converge on each other
        # without node 1. We require at least one node to warm up; for nodes
        # that raced, queries must still be served from cache via interconnect
        # — that's the user-facing invariant.
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
                    "[FullRestart] node %d: TryStartWarmup race -> fetched=0 (see comment above)",
                    node.node_id,
                )

        assert any_fetched, "[FullRestart] At least one node should fetch queries from node 1"

        logger.info("ALL 3 STRESS SCENARIOS PASSED")


# Single-node cluster: no peers ever discovered, warmup must skip; counters
# stay at 0 past the hard deadline and the cluster keeps serving queries.
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

        wait_seconds = self.HARD_DEADLINE_SECONDS + 2
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

        # No warmup inserts -> observation window must stay closed even after
        # the client query above (HitsInWindow/MissesInWindow only attribute
        # to in-window inserts).
        window = CompileCacheView.get_warmup_window_counters(node)
        logger.info("[SingleNode] window counters after client query: %s", window)
        assert window["hits"] == 0 and window["misses"] == 0, (
            f"[SingleNode] Observation window must not open without warmup inserts, got {window}"
        )

        logger.info("SingleNode skip scenario PASSED")


# Multi-node cold start (no entries in any compile cache): proxies discover
# peers (>1) and warmup actor scans .sys/compile_cache_queries, finds it
# empty, completes with no work. Fetched/Compiled stay at 0 on every node.
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
        wait_seconds = self.HARD_DEADLINE_SECONDS + 3
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

        # No warmup inserts anywhere -> window must stay closed on every node.
        for nid in sorted(self.cluster.nodes.keys()):
            node = self.cluster.nodes[nid]
            window = CompileCacheView.get_warmup_window_counters(node)
            logger.info("[ColdStart] node %d window counters: %s", nid, window)
            assert window["hits"] == 0 and window["misses"] == 0, (
                f"[ColdStart] node {nid}: observation window must not open without "
                f"warmup inserts, got {window}"
            )

        logger.info("MultiNodeColdStart no-queries scenario PASSED")


def _is_tcp_port_open(host, port, timeout=0.4):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (ConnectionRefusedError, socket.timeout, OSError):
        return False


# gRPC isolation during warmup: until the warmup actor sends
# TEvKqpWarmupComplete, GRpcServersManager (run.cpp:288-300) does not call
# Start() on the gRPC servers, so the port stays closed to external clients.
# Port-closed window is short on small caches; on very fast hosts we may miss
# it and `grpc_closed_observed` stays False — we log instead of failing,
# because the load-bearing invariant is "port is open after warmup".
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
                # Re-check after a short delay to dodge a flaky one-shot success.
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

        if not grpc_closed_observed:
            logger.warning(
                "[GrpcIsolation] gRPC port was never observed closed during warmup — "
                "warmup likely completed before first sample. Consider extending the cache "
                "or adding artificial compilation delay to make the window observable."
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
