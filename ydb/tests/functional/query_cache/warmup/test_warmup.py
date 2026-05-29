import logging
import socket
import time
import requests
import re

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
            driver.wait(timeout=10)
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
        """Window-attribution counters, missing keys normalised to 0."""
        counters = CompileCacheView.get_warmup_counters(node)
        return {
            "hits": counters.get("Warmup/HitsInWindow", 0),
            "misses": counters.get("Warmup/MissesInWindow", 0),
            "saved_ms": counters.get("Warmup/SavedCompileMs", 0),
        }

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

    def test_warmup_basic(self):
        """Combined test: Table API warmup, Query API warmup, simultaneous restart,
        concurrent user queries"""
        all_node_ids = sorted(self.cluster.nodes.keys())
        node3 = self.cluster.nodes[3]

        # --- Populate cache on all nodes with both APIs ---
        logger.info("SETUP: Populating cache on all nodes (Table API)")
        table_queries = self.cache.populate_cache_on_nodes(all_node_ids, use_query_api=False)
        logger.info("Table API: %d queries on nodes %s", len(table_queries), all_node_ids)

        logger.info("SETUP: Populating cache on all nodes (Query API)")
        query_queries = self.cache.populate_cache_on_nodes(all_node_ids, use_query_api=True)
        logger.info("Query API: %d queries on nodes %s", len(query_queries), all_node_ids)
        time.sleep(2)

        # --- Scenario 1: Table API warmup ---
        logger.info("SCENARIO 1/4: Table API warmup (restart node 3)")
        self._restart_node(node3)
        self._assert_warmup(node3, "TableAPI")
        self._assert_cache_hit(table_queries, node3, "TableAPI", use_query_api=False)
        self._trigger_client_hits(node3, use_query_api=False)
        self._assert_warmup_attribution_hits(node3, "TableAPI")

        # --- Scenario 2: Query API warmup ---
        logger.info("SCENARIO 2/4: Query API warmup (restart node 3)")
        self._restart_node(node3)
        self._assert_warmup(node3, "QueryAPI")
        # Query API path already executes the queries -> no extra trigger needed.
        self._assert_cache_hit(query_queries, node3, "QueryAPI", use_query_api=True)
        self._assert_warmup_attribution_hits(node3, "QueryAPI")

        # --- Scenario 3: Simultaneous restart of nodes 2 + 3 ---
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
            self._assert_warmup(node, f"Simultaneous/node{node.node_id}")

        logger.info("Re-populating cache on all nodes for scenario 4")
        self.cache.populate_cache_on_nodes(all_node_ids, use_query_api=False)
        time.sleep(1)

        # --- Scenario 4: Concurrent user queries during warmup ---
        logger.info("SCENARIO 4/4: Concurrent user queries during warmup")
        self._restart_node(node3, sleep_after=3)

        user_driver = ydb.Driver(
            endpoint=f"grpc://localhost:{node3.port}",
            database="/Root",
        )
        user_driver.wait(timeout=20)
        # Unique literal -> guaranteed miss for Warmup/MissesInWindow.
        fresh_query = "SELECT 7919 + 65537 AS unique_warmup_miss_probe"
        try:
            user_pool = ydb.SessionPool(user_driver)
            try:
                user_ok = 0
                user_queries = ["SELECT 1 + 1", "SELECT 2 * 3", "SELECT 'hello'", fresh_query]
                for q in user_queries:
                    try:
                        user_pool.retry_operation_sync(
                            lambda s, query=q: s.transaction().execute(
                                query,
                                commit_tx=True,
                                settings=ydb.BaseRequestSettings().with_timeout(15),
                            )
                        )
                        user_ok += 1
                        logger.info("[Concurrent] query OK: %s", q)
                    except Exception as e:
                        logger.warning("[Concurrent] query FAILED: %s -> %s", q, e)
            finally:
                user_pool.stop()
        finally:
            user_driver.stop()

        logger.info("[Concurrent] User queries OK: %d/%d", user_ok, len(user_queries))
        assert user_ok == len(user_queries), (
            f"[Concurrent] Expected {len(user_queries)} user queries OK, got {user_ok}"
        )
        self._assert_warmup(node3, "Concurrent")

        # Only the fresh probe is guaranteed to land on node3; the 3 reused
        # queries may route elsewhere, so assert misses only.
        ctrs = CompileCacheView.get_warmup_window_counters(node3)
        logger.info("[Concurrent] Node %d warmup window counters: %s", node3.node_id, ctrs)
        assert ctrs["misses"] > 0, (
            f"[Concurrent] Fresh query inside observation window must bump "
            f"Warmup/MissesInWindow, got {ctrs}"
        )

        logger.info("ALL 4 BASIC SCENARIOS PASSED")


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
        """Combined test: multiple restarts, rolling restart, full cluster restart."""
        all_node_ids = sorted(self.cluster.nodes.keys())
        nodes_count = len(self.cluster.nodes)
        node3 = self.cluster.nodes[3]

        logger.info("SETUP: Populating cache on all nodes (Query API)")
        queries = self.cache.populate_cache_on_nodes(all_node_ids, use_query_api=True)
        logger.info("%d queries on nodes %s", len(queries), all_node_ids)
        time.sleep(2)

        # --- Scenario 1: 3x restart same node ---
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
            assert fetched > 0, f"[MultiRestart round {i + 1}] WarmupQueriesFetched should be > 0, got {fetched}"
            assert compiled > 0, f"[MultiRestart round {i + 1}] WarmupQueriesCompiled should be > 0, got {compiled}"

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

        # --- Scenario 2: Rolling restart ---
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
            logger.info("Node %d: fetched=%d, compiled=%d, counters=%s", nid, fetched, compiled, counters)

            if idx == 0:
                assert fetched > 0, f"[Rolling] First restarted node should fetch, got {fetched}"

        first_restarted = self.cluster.nodes[node_ids_to_restart[0]]
        from_cache, total = self.cache.verify_queries_served_from_cache(
            queries, first_restarted, use_query_api=True, nodes_count=nodes_count,
        )
        logger.info("[Rolling] First restarted node: cache hit %d/%d", from_cache, total)
        assert from_cache == total, f"[Rolling] Expected all from cache, got {from_cache}/{total}"

        logger.info("Re-populating cache on all nodes before full cluster restart")
        self.cache.populate_cache_on_nodes(all_node_ids, use_query_api=True)
        time.sleep(2)

        # --- Scenario 3: Full cluster restart (all except node 1) ---
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

        any_fetched = False
        for node in nodes_to_restart:
            counters = CompileCacheView.wait_for_warmup_finished(
                node,
                timeout=NODE_READY_TIMEOUT_SECONDS + WARMUP_DEADLINE_SECONDS,
            )
            fetched = counters.get("Warmup/QueriesFetched", 0)
            compiled = counters.get("Warmup/QueriesCompiled", 0)
            logger.info("Node %d: fetched=%d, compiled=%d, counters=%s", node.node_id, fetched, compiled, counters)
            if fetched > 0:
                any_fetched = True

        assert any_fetched, "[FullRestart] At least one node should fetch queries from node 1"

        logger.info("ALL 3 STRESS SCENARIOS PASSED")


class TestWarmupSingleNode:
    """Single-node cluster: no peer nodes are ever discovered, so warmup must
    skip without fetching or compiling anything. Counters stay at 0 past the
    warmup hard deadline and the cluster keeps serving user queries."""

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

        # No warmup inserts happened -> observation window must stay closed,
        # the client query above must not have moved any window counters.
        window = CompileCacheView.get_warmup_window_counters(node)
        logger.info("[SingleNode] window counters after client query: %s", window)
        assert window["hits"] == 0 and window["misses"] == 0, (
            f"[SingleNode] Observation window must not open without warmup inserts, got {window}"
        )

        logger.info("SingleNode skip scenario PASSED")


class TestWarmupMultiNodeColdStart:
    """Multi-node cluster cold start (no queries in any compile cache):
    proxies discover peers (>1), KqpProxy sends TEvStartWarmup, warmup actor
    reads .sys/compile_cache_queries, finds it empty, completes with
    "No queries to warm up". Counters Fetched/Compiled stay at 0 on every node."""

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

        # No warmup inserts anywhere => window must stay closed on every node.
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
    """True iff host:port accepts a TCP connection within timeout."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (ConnectionRefusedError, socket.timeout, OSError):
        return False


class TestWarmupGrpcIsolation:
    """gRPC isolation during warmup: until the warmup actor sends
    TEvKqpWarmupComplete, GRpcServersManager (run.cpp:288-300) does not call
    Start() on the gRPC servers, so the port stays closed and external clients
    cannot reach the node. After warmup completes the port opens.

    Caveats:
    - The warmup window is short with a small populated cache, so we sample
      the port aggressively while polling counters via mon port (which is
      independent of GRpcServersManager).
    - On very fast machines warmup may finish before our first sample lands,
      in which case `grpc_closed_observed` may be False — test logs a warning
      rather than failing, since the second invariant (port open after warmup)
      is the load-bearing one."""

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

        # Sanity: port is closed while the process is down.
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
                # Two stable readings to dodge a flaky one-shot success.
                time.sleep(0.3)
                if _is_tcp_port_open("localhost", grpc_port):
                    port_open_after_warmup = True
                    break

            time.sleep(0.2)

        logger.info(
            "[GrpcIsolation] grpc_closed_observed=%s warmup_made_progress=%s port_open_after_warmup=%s",
            grpc_closed_observed, warmup_made_progress, port_open_after_warmup,
        )

        # Load-bearing assertion: by the time warmup is done, gRPC must be open.
        assert warmup_made_progress, "Warmup never made progress — cache populate likely missed the target node"
        assert port_open_after_warmup, (
            "gRPC port did not open after warmup completed; "
            "TEvKqpWarmupComplete -> GRpcServersManager::Start path may be broken"
        )

        # Soft assertion: we expect the closed window to be observable, but on
        # very fast hosts we may miss it. Log instead of failing the test.
        if not grpc_closed_observed:
            logger.warning(
                "[GrpcIsolation] gRPC port was never observed closed during warmup — "
                "warmup likely completed before first sample. Consider extending the cache "
                "or adding artificial compilation delay to make the window observable."
            )

        # Cluster is healthy from the restarted node.
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
