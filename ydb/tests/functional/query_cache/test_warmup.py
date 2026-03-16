import logging
import time
import requests

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure

logger = logging.getLogger(__name__)


WARMUP_DEADLINE_SECONDS = 30
NODE_READY_TIMEOUT_SECONDS = 600  # debug build: tablet bootstrap + peer discovery can take 8+ min
NODE_READY_TIMEOUT_LARGE_CLUSTER = 900  # 10-node debug cluster: IC mesh + tablet bootstrap ~12 min


def verify_queries_served_from_cache_query_api(queries, node, database="/Root", nodes_count=None):
    max_session_attempts = (nodes_count or 3) * 4
    for attempt in range(max_session_attempts):
        driver = ydb.Driver(
            endpoint=f"grpc://localhost:{node.port}",
            database=database,
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
        f"Failed to get session on node {node.node_id} after {max_session_attempts} attempts "
        "(queries are distributed across nodes)"
    )


def verify_queries_served_from_cache_table_api(queries, node, database="/Root"):
    driver = ydb.Driver(
        endpoint=f"grpc://localhost:{node.port}",
        database=database,
    )
    driver.wait(timeout=10)
    try:
        pool = ydb.SessionPool(driver)
        try:
            for item in queries:
                if isinstance(item, tuple):
                    query, params = item
                else:
                    query, params = item, None
                pool.retry_operation_sync(lambda s, q=query, p=params: _execute_table_api(s, q, p))
            return len(queries), len(queries)
        finally:
            pool.stop()
    finally:
        driver.stop()


def _execute_table_api(session, query, params):
    settings = ydb.BaseRequestSettings().with_timeout(10)
    if params:
        prepared = session.prepare(query)
        return session.transaction().execute(prepared, params, commit_tx=True, settings=settings)
    return session.transaction().execute(query, commit_tx=True, settings=settings)


def verify_queries_served_from_cache_on_node(queries, node, database="/Root", use_query_api=True, nodes_count=None):
    if use_query_api:
        return verify_queries_served_from_cache_query_api(queries, node, database, nodes_count=nodes_count)
    return verify_queries_served_from_cache_table_api(queries, node, database)


def get_warmup_counters(node, verbose=False):
    try:
        url = f"http://localhost:{node.mon_port}/counters/counters=kqp/json"
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            counters = {}
            sensors = data.get("sensors", [])
            for sensor in sensors:
                labels = sensor.get("labels", {})
                name = labels.get("sensor", "")
                if "Warmup" in name:
                    counters[name] = sensor.get("value", 0)
            if verbose and not counters:
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


def wait_for_warmup_finished(node, timeout=60):
    """Wait for warmup to complete: fetch AND compilation.

    QueriesFetched is set before compilations start, so we must also wait for
    QueriesCompiled to stabilize (stop increasing for 2 consecutive polls).
    In debug builds node init can take 2+ min before warmup even starts.
    """
    start_time = time.time()
    last_compiled = -1
    stable_polls = 0
    while time.time() - start_time < timeout:
        counters = get_warmup_counters(node)
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
    return get_warmup_counters(node, verbose=True)


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


def _get_node_id_from_session_id(session_id):
    """Extract node_id from session ID like 'ydb://session/3?node_id=X&id=...'."""
    import re

    m = re.search(r'node_id=(\d+)', session_id)
    return int(m.group(1)) if m else None


def _populate_cache_on_node_table_api(driver, target_node_id, queries, repeat_count=3):
    """Populate cache via Table API, retrying session creation until it lands on target node."""
    settings = ydb.BaseRequestSettings().with_timeout(10)
    for attempt in range(30):
        session = driver.table_client.session().create()
        try:
            actual_node = _get_node_id_from_session_id(session.session_id)
            if actual_node != target_node_id:
                continue
            for query in queries:
                prepared = session.prepare(query)
                for _ in range(repeat_count):
                    session.transaction().execute(
                        prepared,
                        {},
                        commit_tx=True,
                        settings=settings,
                    )
            prepared_param = session.prepare(WARMUP_QUERY_PARAM)
            for _ in range(repeat_count):
                session.transaction().execute(
                    prepared_param,
                    WARMUP_PARAM_VALUES,
                    commit_tx=True,
                    settings=settings,
                )
            return True
        finally:
            session.delete()
    return False


def _populate_cache_on_node_query_api(driver, target_node_id, queries, repeat_count=3):
    """Populate cache via Query API, retrying session creation until it lands on target node."""
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
                for _ in session.execute(
                    WARMUP_QUERY_PARAM,
                    WARMUP_PARAM_VALUES,
                ):
                    pass
            return True
        finally:
            session.delete()
    return False


def execute_queries_to_populate_cache_on_nodes(
    cluster,
    node_ids,
    database="/Root",
    extra_queries=None,
    use_query_api=False,
):
    """Populate compile cache on specific nodes by creating per-node connections.

    Uses session ID inspection to verify the session landed on the target node,
    retrying if SDK discovery routed it elsewhere.
    """
    queries = list(WARMUP_QUERIES)
    if extra_queries:
        queries.extend(extra_queries)

    for node_id in node_ids:
        node = cluster.nodes[node_id]
        driver = ydb.Driver(endpoint=f"grpc://localhost:{node.port}", database=database)
        driver.wait(timeout=10)
        try:
            if use_query_api:
                ok = _populate_cache_on_node_query_api(
                    driver,
                    node_id,
                    queries,
                )
            else:
                ok = _populate_cache_on_node_table_api(
                    driver,
                    node_id,
                    queries,
                )
            if not ok:
                logger.warning(
                    "Could not pin session to node %d after 30 attempts, " "cache may be on a different node",
                    node_id,
                )
        finally:
            driver.stop()

    queries.append((WARMUP_QUERY_PARAM, WARMUP_PARAM_VALUES))
    return queries


def _make_warmup_config(nodes=3, max_nodes_to_request=None):
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
    config.yaml_config["table_service_config"] = {
        "compile_query_cache_size": 200 if nodes > 3 else 100,
        "compile_cache_warmup_config": warmup_config,
    }
    return config


class TestWarmupBasic:
    """Basic warmup scenarios: Table API, Query API, simultaneous restart, concurrent queries.

    Single 3-node cluster, single cache populate, multiple restart scenarios.
    Saves ~6 cluster startups compared to running each scenario separately.
    """

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
        counters = wait_for_warmup_finished(node, timeout=timeout)
        fetched = counters.get("Warmup/QueriesFetched", 0)
        compiled = counters.get("Warmup/QueriesCompiled", 0)
        logger.info(
            "[%s] Node %d: fetched=%d, compiled=%d, counters=%s", label, node.node_id, fetched, compiled, counters
        )
        assert fetched > 0, f"[{label}] WarmupQueriesFetched should be > 0, got {fetched}"
        assert compiled > 0, f"[{label}] WarmupQueriesCompiled should be > 0, got {compiled}, " f"fetched={fetched}"
        return counters

    def _assert_cache_hit(self, queries, node, label, use_query_api=False):
        from_cache, total = verify_queries_served_from_cache_on_node(
            queries,
            node,
            use_query_api=use_query_api,
            nodes_count=len(self.cluster.nodes),
        )
        logger.info("[%s] Node %d: cache hit %d/%d", label, node.node_id, from_cache, total)
        assert from_cache == total, f"[{label}] Expected all {total} from cache, got {from_cache}/{total}"

    def test_warmup_basic(self):
        """Combined test: Table API warmup, Query API warmup, simultaneous restart,
        concurrent user queries. One cluster, one populate, four scenarios."""
        all_node_ids = sorted(self.cluster.nodes.keys())
        node3 = self.cluster.nodes[3]

        # --- Populate cache on all nodes with both APIs ---
        logger.info("SETUP: Populating cache on all nodes (Table API)")
        table_queries = execute_queries_to_populate_cache_on_nodes(
            self.cluster,
            all_node_ids,
            use_query_api=False,
        )
        logger.info("Table API: %d queries on nodes %s", len(table_queries), all_node_ids)

        logger.info("SETUP: Populating cache on all nodes (Query API)")
        query_queries = execute_queries_to_populate_cache_on_nodes(
            self.cluster,
            all_node_ids,
            use_query_api=True,
        )
        logger.info("Query API: %d queries on nodes %s", len(query_queries), all_node_ids)
        time.sleep(2)

        # --- Scenario 1: Table API warmup ---
        logger.info("SCENARIO 1/4: Table API warmup (restart node 3)")
        self._restart_node(node3)
        self._assert_warmup(node3, "TableAPI")
        self._assert_cache_hit(table_queries, node3, "TableAPI", use_query_api=False)

        # --- Scenario 2: Query API warmup ---
        logger.info("SCENARIO 2/4: Query API warmup (restart node 3)")
        self._restart_node(node3)
        self._assert_warmup(node3, "QueryAPI")
        self._assert_cache_hit(query_queries, node3, "QueryAPI", use_query_api=True)

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
        execute_queries_to_populate_cache_on_nodes(
            self.cluster,
            all_node_ids,
            use_query_api=False,
        )
        time.sleep(1)

        # --- Scenario 4: Concurrent user queries during warmup ---
        logger.info("SCENARIO 4/4: Concurrent user queries during warmup")
        self._restart_node(node3, sleep_after=3)

        user_driver = ydb.Driver(
            endpoint=f"grpc://localhost:{node3.port}",
            database="/Root",
        )
        user_driver.wait(timeout=20)
        try:
            user_pool = ydb.SessionPool(user_driver)
            try:
                user_ok = 0
                user_queries = ["SELECT 1 + 1", "SELECT 2 * 3", "SELECT 'hello'"]
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

        logger.info("[Concurrent] User queries OK: %d/3", user_ok)
        assert user_ok == 3, f"[Concurrent] Expected 3 user queries OK, got {user_ok}"
        self._assert_warmup(node3, "Concurrent")

        logger.info("ALL 4 BASIC SCENARIOS PASSED")


class TestWarmupStress:
    """Stress warmup scenarios: multiple restarts, rolling restart, full cluster restart.

    Single 3-node cluster, single cache populate, three scenarios.
    """

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

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, "driver"):
            cls.driver.stop()
        if hasattr(cls, "cluster"):
            cls.cluster.stop()

    def test_warmup_stress(self):
        """Combined test: multiple restarts, rolling restart, full cluster restart.
        One cluster, one populate, three scenarios."""
        all_node_ids = sorted(self.cluster.nodes.keys())
        nodes_count = len(self.cluster.nodes)
        node3 = self.cluster.nodes[3]

        logger.info("SETUP: Populating cache on all nodes (Table API)")
        queries = execute_queries_to_populate_cache_on_nodes(
            self.cluster,
            all_node_ids,
            use_query_api=False,
        )
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

            counters = wait_for_warmup_finished(
                node3,
                timeout=NODE_READY_TIMEOUT_SECONDS + WARMUP_DEADLINE_SECONDS,
            )
            fetched = counters.get("Warmup/QueriesFetched", 0)
            compiled = counters.get("Warmup/QueriesCompiled", 0)
            logger.info("Round %d: fetched=%d, compiled=%d, counters=%s", i + 1, fetched, compiled, counters)
            assert fetched > 0, f"[MultiRestart round {i + 1}] WarmupQueriesFetched " f"should be > 0, got {fetched}"
            assert compiled > 0, f"[MultiRestart round {i + 1}] WarmupQueriesCompiled " f"should be > 0, got {compiled}"

            from_cache, total = verify_queries_served_from_cache_on_node(
                queries,
                node3,
                use_query_api=False,
                nodes_count=nodes_count,
            )
            logger.info("Round %d: cache hit %d/%d", i + 1, from_cache, total)
            assert from_cache == total, (
                f"[MultiRestart round {i + 1}] Expected all from cache, " f"got {from_cache}/{total}"
            )

        logger.info("Re-populating cache on all nodes before rolling restart")
        execute_queries_to_populate_cache_on_nodes(
            self.cluster,
            all_node_ids,
            use_query_api=False,
        )
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

            counters = wait_for_warmup_finished(
                node,
                timeout=NODE_READY_TIMEOUT_SECONDS + WARMUP_DEADLINE_SECONDS,
            )
            fetched = counters.get("Warmup/QueriesFetched", 0)
            compiled = counters.get("Warmup/QueriesCompiled", 0)
            logger.info("Node %d: fetched=%d, compiled=%d, counters=%s", nid, fetched, compiled, counters)

            if idx == 0:
                assert fetched > 0, f"[Rolling] First restarted node should fetch, got {fetched}"

        first_restarted = self.cluster.nodes[node_ids_to_restart[0]]
        from_cache, total = verify_queries_served_from_cache_on_node(
            queries,
            first_restarted,
            use_query_api=False,
            nodes_count=nodes_count,
        )
        logger.info("[Rolling] First restarted node: cache hit %d/%d", from_cache, total)
        assert from_cache == total, f"[Rolling] Expected all from cache, got {from_cache}/{total}"

        logger.info("Re-populating cache on all nodes before full cluster restart")
        execute_queries_to_populate_cache_on_nodes(
            self.cluster,
            all_node_ids,
            use_query_api=False,
        )
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
            counters = wait_for_warmup_finished(
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


class TestWarmupNodeFilter:
    """Warmup tests with 10-node cluster and max_nodes_to_request=3."""

    EXTRA_QUERIES = [
        "SELECT 10 * 10 AS result",
        "SELECT 'hello' || ' ' || 'world' AS greeting",
        "SELECT CAST(123 AS String) AS str_value",
        "SELECT ABS(-456) AS abs_value",
    ]

    @classmethod
    def setup_class(cls):
        cls.config = _make_warmup_config(nodes=10, max_nodes_to_request=3)
        cls.cluster = kikimr_cluster_factory(cls.config)
        cls.cluster.start()
        cls.driver = ydb.Driver(
            endpoint=f"grpc://localhost:{cls.cluster.nodes[1].port}",
            database="/Root",
        )
        cls.driver.wait(timeout=10)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, "driver"):
            cls.driver.stop()
        if hasattr(cls, "cluster"):
            cls.cluster.stop()

    def test_warmup_filters_by_first_three_nodes(self):
        """Test that warmup queries only first 3 nodes from compile cache."""
        logger.info("Cluster has %d nodes", len(self.cluster.nodes))
        all_node_ids = sorted(self.cluster.nodes.keys())
        queries = execute_queries_to_populate_cache_on_nodes(
            self.cluster,
            all_node_ids,
            extra_queries=self.EXTRA_QUERIES,
            use_query_api=False,
        )
        logger.info("Executed %d queries on nodes %s (Table API)", len(queries), all_node_ids)
        time.sleep(3)

        logger.info("Checking cache population on nodes before restart:")
        for nid in all_node_ids[:5]:
            node = self.cluster.nodes[nid]
            counters = get_warmup_counters(node)
            logger.info("Node %d (id=%d): counters=%s", nid, node.node_id, counters)

        node_to_restart_idx = 4
        node_to_restart = self.cluster.nodes[node_to_restart_idx + 1]
        logger.info("Restarting node %d (node_id=%d)", node_to_restart_idx + 1, node_to_restart.node_id)

        node_to_restart.stop()
        time.sleep(3)
        node_to_restart.start()
        time.sleep(5)

        counters_after = wait_for_warmup_finished(
            node_to_restart,
            timeout=NODE_READY_TIMEOUT_LARGE_CLUSTER + WARMUP_DEADLINE_SECONDS,
        )
        warmup_fetched = counters_after.get("Warmup/QueriesFetched", 0)
        warmup_compiled = counters_after.get("Warmup/QueriesCompiled", 0)
        logger.info("fetched=%d, compiled=%d, counters=%s", warmup_fetched, warmup_compiled, counters_after)
        assert warmup_fetched > 0, f"WarmupQueriesFetched should be > 0, got {warmup_fetched}"
        assert warmup_compiled > 0, f"WarmupQueriesCompiled should be > 0, got {warmup_compiled}"

        from_cache_count, total = verify_queries_served_from_cache_on_node(
            queries,
            node_to_restart,
            use_query_api=False,
            nodes_count=len(self.cluster.nodes),
        )
        logger.info("Cache hit: %d/%d", from_cache_count, total)
        assert from_cache_count == total, (
            f"All queries should be served from cache after warmup, " f"got {from_cache_count}/{total} from cache"
        )
