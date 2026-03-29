#!/usr/bin/env python

import logging
import re
import time

import pytest
import requests

from hamcrest import assert_that, equal_to, greater_than

from ydb.tests.library.common.wait_for import wait_for


logger = logging.getLogger(__name__)

TARGET = "harmonizer.max_used_cpu_x1e6"
AWAKENING_TARGET = "harmonizer.avg_awakening_time_us"
POOL_TARGET = "harmonizer.pool.avg_used_cpu_x1e6"
PROMETHEUS_API_PREFIX = "/viewer/inmemory_metrics/prometheus/api/v1"
MEMORY_USED_TARGET = "inmemory_metrics.memory_used_bytes"
COMMITTED_BYTES_TARGET = "inmemory_metrics.committed_bytes"
FREE_CHUNKS_TARGET = "inmemory_metrics.free_chunks"
USED_CHUNKS_TARGET = "inmemory_metrics.used_chunks"
SEALED_CHUNKS_TARGET = "inmemory_metrics.sealed_chunks"
WRITABLE_CHUNKS_TARGET = "inmemory_metrics.writable_chunks"
RETIRING_CHUNKS_TARGET = "inmemory_metrics.retiring_chunks"
LINES_TARGET = "inmemory_metrics.lines"
CLOSED_LINES_TARGET = "inmemory_metrics.closed_lines"
REUSE_WATERMARK_TARGET = "inmemory_metrics.reuse_watermark"
APPEND_FAILURES_TOTAL_TARGET = "inmemory_metrics.append_failures_total"


def wait_for_target(base_url, predicate, prefix="harmonizer"):
    last_targets = []

    def targets_ready():
        nonlocal last_targets
        try:
            response = requests.get(
                f"{base_url}/viewer/inmemory_metrics/targets",
                params={"prefix": prefix},
                timeout=10,
            )
        except requests.exceptions.RequestException:
            return False

        if response.status_code != 200:
            return False

        last_targets = response.json()
        logger.info("inmemory metric targets: %s", last_targets)
        return any(predicate(target) for target in last_targets)

    assert wait_for(targets_ready, timeout_seconds=30, step_seconds=1.0), last_targets
    return next(target for target in last_targets if predicate(target))


def extract_label(target, label_name):
    match = re.search(rf'{re.escape(label_name)}="([^"]+)"', target)
    assert match, target
    return match.group(1)


def get_prometheus_json(base_url, path, params=None):
    try:
        response = requests.get(
            f"{base_url}{PROMETHEUS_API_PREFIX}{path}",
            params=params,
            timeout=10,
        )
    except requests.exceptions.RequestException:
        return None

    if response.status_code != 200:
        return None

    payload = response.json()
    if payload.get("status") != "success":
        return None

    return payload.get("data")


def test_inmemory_metrics_are_exposed(ydb_cluster):
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f"http://localhost:{mon_port}"
    target = wait_for_target(
        base_url,
        lambda metric: metric.startswith(f'{TARGET}{{node_id="'),
    )

    last_find = None

    def graphite_find_ready():
        nonlocal last_find
        try:
            response = requests.get(
                f"{base_url}/viewer/inmemory_metrics/metrics/find",
                params={"query": TARGET},
                timeout=10,
            )
        except requests.exceptions.RequestException:
            return False

        if response.status_code != 200:
            return False

        last_find = response.json()
        logger.info("inmemory graphite find: %s", last_find)
        return any(item.get("id") == target for item in last_find)

    assert wait_for(graphite_find_ready, timeout_seconds=30, step_seconds=1.0), last_find

    last_series = None

    def series_ready():
        nonlocal last_series
        try:
            response = requests.get(
                f"{base_url}/viewer/inmemory_metrics/render",
                params={
                    "target": TARGET,
                },
                timeout=10,
            )
        except requests.exceptions.RequestException:
            return False

        if response.status_code != 200:
            return False

        last_series = response.json()
        logger.info("inmemory metric series: %s", last_series)
        return bool(last_series) and bool(last_series[0].get("datapoints"))

    assert wait_for(series_ready, timeout_seconds=30, step_seconds=1.0), last_series

    datapoints = last_series[0]["datapoints"]
    assert_that(last_series[0]["target"], equal_to(target))
    assert "node_id" in last_series[0]["tags"]
    assert_that(len(datapoints), greater_than(0))
    assert_that(datapoints[-1][1], greater_than(0))


def test_inmemory_metrics_registry_meta_lines_are_exposed(ydb_cluster):
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f"http://localhost:{mon_port}"
    target = wait_for_target(
        base_url,
        lambda metric: metric.startswith(f'{MEMORY_USED_TARGET}{{node_id="'),
        prefix="inmemory_metrics",
    )

    last_series = None

    def series_ready():
        nonlocal last_series
        try:
            response = requests.get(
                f"{base_url}/viewer/inmemory_metrics/render",
                params={"target": MEMORY_USED_TARGET},
                timeout=10,
            )
        except requests.exceptions.RequestException:
            return False

        if response.status_code != 200:
            return False

        last_series = response.json()
        logger.info("inmemory registry meta series: %s", last_series)
        return (
            bool(last_series)
            and last_series[0].get("target") == target
            and bool(last_series[0].get("datapoints"))
            and last_series[0]["datapoints"][-1][0] >= 0
        )

    assert wait_for(series_ready, timeout_seconds=30, step_seconds=1.0), last_series


def test_inmemory_metrics_render_accepts_labeled_graphite_target(ydb_cluster):
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f"http://localhost:{mon_port}"
    target = wait_for_target(
        base_url,
        lambda metric: metric.startswith('harmonizer.pool.avg_used_cpu_x1e6{node_id="'),
    )

    last_series = None

    def series_ready():
        nonlocal last_series
        try:
            response = requests.get(
                f"{base_url}/viewer/inmemory_metrics/render",
                params={"target": target},
                timeout=10,
            )
        except requests.exceptions.RequestException:
            return False

        if response.status_code != 200:
            return False

        last_series = response.json()
        logger.info("inmemory labeled metric series: %s", last_series)
        return (
            bool(last_series)
            and last_series[0].get("target") == target
            and bool(last_series[0].get("datapoints"))
        )

    assert wait_for(series_ready, timeout_seconds=30, step_seconds=1.0), last_series

    datapoints = last_series[0]["datapoints"]
    assert_that(last_series[0]["target"], equal_to(target))
    assert_that(len(datapoints), greater_than(0))


def test_inmemory_metrics_render_supports_graphite_alias_sub_json_format(ydb_cluster):
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f"http://localhost:{mon_port}"
    metric_target = wait_for_target(
        base_url,
        lambda metric: metric.startswith(f'{AWAKENING_TARGET}{{node_id="'),
    )
    target = 'aliasSub(harmonizer.avg_awakening_time_us, "(^.*$)", "\\1 A")'

    last_series = None

    def series_ready():
        nonlocal last_series
        try:
            response = requests.post(
                f"{base_url}/viewer/inmemory_metrics/render",
                data={
                    "target": target,
                    "format": "json",
                },
                timeout=10,
            )
        except requests.exceptions.RequestException:
            return False

        if response.status_code != 200:
            return False

        last_series = response.json()
        logger.info("inmemory aliased metric series: %s", last_series)
        return (
            bool(last_series)
            and last_series[0].get("target") == f"{metric_target} A"
            and bool(last_series[0].get("datapoints"))
        )

    assert wait_for(series_ready, timeout_seconds=30, step_seconds=1.0), last_series

    datapoints = last_series[0]["datapoints"]
    assert_that(last_series[0]["target"], equal_to(f"{metric_target} A"))
    assert_that(len(datapoints), greater_than(0))


def test_inmemory_metrics_prometheus_label_discovery(ydb_cluster):
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f"http://localhost:{mon_port}"
    target = wait_for_target(
        base_url,
        lambda metric: metric.startswith(f'{TARGET}{{node_id="'),
    )
    node_id = extract_label(target, "node_id")

    pool_target = wait_for_target(
        base_url,
        lambda metric: metric.startswith(f'{POOL_TARGET}{{node_id="'),
    )
    pool_name = extract_label(pool_target, "pool")
    pool_id = extract_label(pool_target, "pool_id")

    last_labels = None

    def labels_ready():
        nonlocal last_labels
        data = get_prometheus_json(base_url, "/labels")
        if not data:
            return False

        last_labels = data
        logger.info("prometheus labels: %s", last_labels)
        return "__name__" in last_labels and "node_id" in last_labels and "pool" in last_labels and "pool_id" in last_labels

    assert wait_for(labels_ready, timeout_seconds=30, step_seconds=1.0), last_labels

    last_metric_names = None

    def metric_names_ready():
        nonlocal last_metric_names
        data = get_prometheus_json(base_url, "/label/__name__/values")
        if not data:
            return False

        last_metric_names = data
        logger.info("prometheus metric names: %s", last_metric_names)
        return (
            TARGET in last_metric_names
            and AWAKENING_TARGET in last_metric_names
            and MEMORY_USED_TARGET in last_metric_names
            and COMMITTED_BYTES_TARGET in last_metric_names
            and FREE_CHUNKS_TARGET in last_metric_names
            and USED_CHUNKS_TARGET in last_metric_names
            and SEALED_CHUNKS_TARGET in last_metric_names
            and WRITABLE_CHUNKS_TARGET in last_metric_names
            and RETIRING_CHUNKS_TARGET in last_metric_names
            and LINES_TARGET in last_metric_names
            and CLOSED_LINES_TARGET in last_metric_names
            and REUSE_WATERMARK_TARGET in last_metric_names
            and APPEND_FAILURES_TOTAL_TARGET in last_metric_names
        )

    assert wait_for(metric_names_ready, timeout_seconds=30, step_seconds=1.0), last_metric_names

    last_node_ids = None

    def node_ids_ready():
        nonlocal last_node_ids
        data = get_prometheus_json(base_url, "/label/node_id/values")
        if not data:
            return False

        last_node_ids = data
        logger.info("prometheus node ids: %s", last_node_ids)
        return node_id in last_node_ids

    assert wait_for(node_ids_ready, timeout_seconds=30, step_seconds=1.0), last_node_ids

    last_pools = None

    def pools_ready():
        nonlocal last_pools
        data = get_prometheus_json(base_url, "/label/pool/values")
        if not data:
            return False

        last_pools = data
        logger.info("prometheus pool labels: %s", last_pools)
        return pool_name in last_pools

    assert wait_for(pools_ready, timeout_seconds=30, step_seconds=1.0), last_pools

    last_pool_ids = None

    def pool_ids_ready():
        nonlocal last_pool_ids
        data = get_prometheus_json(base_url, "/label/pool_id/values")
        if not data:
            return False

        last_pool_ids = data
        logger.info("prometheus pool ids: %s", last_pool_ids)
        return pool_id in last_pool_ids

    assert wait_for(pool_ids_ready, timeout_seconds=30, step_seconds=1.0), last_pool_ids

    last_series = None

    def series_ready():
        nonlocal last_series
        data = get_prometheus_json(
            base_url,
            "/series",
            params={"match[]": f'{TARGET}{{node_id="{node_id}"}}'},
        )
        if not data:
            return False

        last_series = data
        logger.info("prometheus series: %s", last_series)
        return any(
            series.get("__name__") == TARGET and series.get("node_id") == node_id
            for series in last_series
        )

    assert wait_for(series_ready, timeout_seconds=30, step_seconds=1.0), last_series


def test_inmemory_metrics_prometheus_query_selector_only(ydb_cluster):
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f"http://localhost:{mon_port}"
    target = wait_for_target(
        base_url,
        lambda metric: metric.startswith(f'{TARGET}{{node_id="'),
    )

    last_query = None

    def query_ready():
        nonlocal last_query
        data = get_prometheus_json(
            base_url,
            "/query",
            params={
                "query": target,
                "time": int(time.time()),
            },
        )
        if not data:
            return False

        last_query = data
        logger.info("prometheus query result: %s", last_query)
        result = last_query.get("result", [])
        return last_query.get("resultType") == "vector" and any(
            item.get("metric", {}).get("__name__") == TARGET
            and item.get("metric", {}).get("node_id")
            for item in result
        )

    assert wait_for(query_ready, timeout_seconds=30, step_seconds=1.0), last_query


def test_inmemory_metrics_prometheus_query_grafana_quoted_metric_selector(ydb_cluster):
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f"http://localhost:{mon_port}"
    target = wait_for_target(
        base_url,
        lambda metric: metric.startswith(f'{POOL_TARGET}{{node_id="'),
    )
    node_id = extract_label(target, "node_id")
    pool_name = extract_label(target, "pool")

    last_query = None

    def query_ready():
        nonlocal last_query
        data = get_prometheus_json(
            base_url,
            "/query",
            params={
                "query": f'{{"{POOL_TARGET}", node_id="{node_id}", pool="{pool_name}"}}',
                "time": int(time.time()),
            },
        )
        if not data:
            return False

        last_query = data
        logger.info("prometheus query result for quoted metric selector: %s", last_query)
        result = last_query.get("result", [])
        return last_query.get("resultType") == "vector" and any(
            item.get("metric", {}).get("__name__") == POOL_TARGET
            and item.get("metric", {}).get("node_id") == node_id
            and item.get("metric", {}).get("pool") == pool_name
            for item in result
        )

    assert wait_for(query_ready, timeout_seconds=30, step_seconds=1.0), last_query


def test_inmemory_metrics_prometheus_query_supports_count_by_aggregation(ydb_cluster):
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f"http://localhost:{mon_port}"
    target = wait_for_target(
        base_url,
        lambda metric: metric.startswith(f'{POOL_TARGET}{{node_id="'),
    )
    node_id = extract_label(target, "node_id")

    last_query = None

    def query_ready():
        nonlocal last_query
        data = get_prometheus_json(
            base_url,
            "/query",
            params={
                "query": f'count by (node_id) ({POOL_TARGET}{{node_id="{node_id}"}})',
                "time": int(time.time()),
            },
        )
        if not data:
            return False

        last_query = data
        logger.info("prometheus aggregation query result: %s", last_query)
        result = last_query.get("result", [])
        return last_query.get("resultType") == "vector" and any(
            item.get("metric", {}).get("node_id") == node_id
            and "__name__" not in item.get("metric", {})
            and "pool" not in item.get("metric", {})
            and float(item.get("value", [0, "0"])[1]) > 0
            for item in result
        )

    assert wait_for(query_ready, timeout_seconds=30, step_seconds=1.0), last_query


def test_inmemory_metrics_prometheus_query_supports_last_over_time(ydb_cluster):
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f"http://localhost:{mon_port}"
    target = wait_for_target(
        base_url,
        lambda metric: metric.startswith(f'{TARGET}{{node_id="'),
    )

    last_query = None

    def query_ready():
        nonlocal last_query
        data = get_prometheus_json(
            base_url,
            "/query",
            params={
                "query": f"last_over_time({target}[5m])",
                "time": int(time.time()),
            },
        )
        if not data:
            return False

        last_query = data
        logger.info("prometheus last_over_time query result: %s", last_query)
        result = last_query.get("result", [])
        return last_query.get("resultType") == "vector" and any(
            item.get("metric", {}).get("__name__") == TARGET
            and item.get("metric", {}).get("node_id")
            for item in result
        )

    assert wait_for(query_ready, timeout_seconds=30, step_seconds=1.0), last_query


def test_inmemory_metrics_prometheus_query_range_selector_only(ydb_cluster):
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f"http://localhost:{mon_port}"
    target = wait_for_target(
        base_url,
        lambda metric: metric.startswith(f'{TARGET}{{node_id="'),
    )

    last_query_range = None
    end = int(time.time())
    start = end - 300

    def query_range_ready():
        nonlocal last_query_range
        data = get_prometheus_json(
            base_url,
            "/query_range",
            params={
                "query": target,
                "start": start,
                "end": end,
                "step": 30,
            },
        )
        if not data:
            return False

        last_query_range = data
        logger.info("prometheus query_range result: %s", last_query_range)
        result = last_query_range.get("result", [])
        return last_query_range.get("resultType") == "matrix" and any(
            item.get("metric", {}).get("__name__") == TARGET
            and item.get("metric", {}).get("node_id")
            and item.get("values")
            for item in result
        )

    assert wait_for(query_range_ready, timeout_seconds=30, step_seconds=1.0), last_query_range


def test_inmemory_metrics_prometheus_query_range_self_metric_tail_reaches_range_end(ydb_cluster):
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f"http://localhost:{mon_port}"
    target = wait_for_target(
        base_url,
        lambda metric: metric.startswith(f'{USED_CHUNKS_TARGET}{{node_id="'),
        prefix="inmemory_metrics",
    )

    def get_last_timestamp(query_end):
        data = get_prometheus_json(
            base_url,
            "/query_range",
            params={
                "query": target,
                "start": query_end - 300,
                "end": query_end,
                "step": 30,
            },
        )
        assert data is not None
        logger.info("prometheus self metric query_range result: %s", data)
        assert_that(data.get("resultType"), equal_to("matrix"))

        for item in data.get("result", []):
            metric = item.get("metric", {})
            if metric.get("__name__") != USED_CHUNKS_TARGET or metric.get("node_id") is None:
                continue
            values = item.get("values", [])
            if values:
                return values[-1][0]

        return None

    first_end = int(time.time())
    first_last = None

    def first_query_ready():
        nonlocal first_last
        first_last = get_last_timestamp(first_end)
        return first_last is not None and first_last >= first_end - 5

    assert wait_for(first_query_ready, timeout_seconds=30, step_seconds=1.0), first_last

    time.sleep(2)

    second_end = int(time.time())
    second_last = None

    def second_query_ready():
        nonlocal second_last
        second_last = get_last_timestamp(second_end)
        return second_last is not None and second_last >= second_end - 5 and second_last > first_last

    assert wait_for(second_query_ready, timeout_seconds=30, step_seconds=1.0), {
        "first_last": first_last,
        "second_last": second_last,
    }


def test_inmemory_metrics_prometheus_query_range_self_metric_fills_heartbeat_gaps(ydb_cluster):
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f"http://localhost:{mon_port}"
    target = wait_for_target(
        base_url,
        lambda metric: metric.startswith(f'{USED_CHUNKS_TARGET}{{node_id="'),
        prefix="inmemory_metrics",
    )

    last_query_range = None

    def query_range_ready():
        nonlocal last_query_range
        end = int(time.time())
        data = get_prometheus_json(
            base_url,
            "/query_range",
            params={
                "query": target,
                "start": end - 60,
                "end": end,
                "step": 10,
            },
        )
        if not data:
            return False

        last_query_range = data
        logger.info("prometheus self metric gap-filled query_range result: %s", last_query_range)
        if last_query_range.get("resultType") != "matrix":
            return False

        for item in last_query_range.get("result", []):
            metric = item.get("metric", {})
            if metric.get("__name__") != USED_CHUNKS_TARGET or metric.get("node_id") is None:
                continue

            values = item.get("values", [])
            if len(values) < 5:
                continue

            timestamps = [value[0] for value in values]
            if timestamps[-1] < end - 10:
                continue

            step_diffs = [rhs - lhs for lhs, rhs in zip(timestamps, timestamps[1:])]
            return all(diff == 10 for diff in step_diffs[-4:])

        return False

    assert wait_for(query_range_ready, timeout_seconds=30, step_seconds=1.0), last_query_range


def test_inmemory_metrics_prometheus_query_range_supports_last_over_time(ydb_cluster):
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f"http://localhost:{mon_port}"
    target = wait_for_target(
        base_url,
        lambda metric: metric.startswith(f'{TARGET}{{node_id="'),
    )

    last_query_range = None
    end = int(time.time())
    start = end - 300

    def query_range_ready():
        nonlocal last_query_range
        data = get_prometheus_json(
            base_url,
            "/query_range",
            params={
                "query": f"last_over_time({target}[5m])",
                "start": start,
                "end": end,
                "step": "30s",
            },
        )
        if not data:
            return False

        last_query_range = data
        logger.info("prometheus last_over_time query_range result: %s", last_query_range)
        result = last_query_range.get("result", [])
        return last_query_range.get("resultType") == "matrix" and any(
            item.get("metric", {}).get("__name__") == TARGET
            and item.get("metric", {}).get("node_id")
            and item.get("values")
            for item in result
        )

    assert wait_for(query_range_ready, timeout_seconds=30, step_seconds=1.0), last_query_range


def test_inmemory_metrics_prometheus_query_range_supports_count_by_aggregation(ydb_cluster):
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f"http://localhost:{mon_port}"
    target = wait_for_target(
        base_url,
        lambda metric: metric.startswith(f'{POOL_TARGET}{{node_id="'),
    )
    node_id = extract_label(target, "node_id")

    last_query_range = None
    end = int(time.time())
    start = end - 300

    def query_range_ready():
        nonlocal last_query_range
        data = get_prometheus_json(
            base_url,
            "/query_range",
            params={
                "query": f'count by (node_id) ({POOL_TARGET}{{node_id="{node_id}"}})',
                "start": start,
                "end": end,
                "step": 30,
            },
        )
        if not data:
            return False

        last_query_range = data
        logger.info("prometheus aggregation query_range result: %s", last_query_range)
        result = last_query_range.get("result", [])
        return last_query_range.get("resultType") == "matrix" and any(
            item.get("metric", {}).get("node_id") == node_id
            and "__name__" not in item.get("metric", {})
            and "pool" not in item.get("metric", {})
            and item.get("values")
            for item in result
        )

    assert wait_for(query_range_ready, timeout_seconds=30, step_seconds=1.0), last_query_range


def test_inmemory_metrics_prometheus_query_routes_to_exact_remote_node(ydb_cluster):
    if len(ydb_cluster.nodes) < 2:
        pytest.skip("requires at least two nodes")

    source_base_url = f"http://localhost:{ydb_cluster.nodes[1].mon_port}"
    target_base_url = f"http://localhost:{ydb_cluster.nodes[2].mon_port}"
    target = wait_for_target(
        target_base_url,
        lambda metric: metric.startswith(f'{TARGET}{{node_id="'),
    )
    remote_node_id = extract_label(target, "node_id")

    last_query = None

    def query_ready():
        nonlocal last_query
        data = get_prometheus_json(
            source_base_url,
            "/query",
            params={
                "query": f'{{"{TARGET}", node_id="{remote_node_id}"}}',
                "time": int(time.time()),
            },
        )
        if not data:
            return False

        last_query = data
        logger.info("prometheus remote node query result: %s", last_query)
        result = last_query.get("result", [])
        return last_query.get("resultType") == "vector" and any(
            item.get("metric", {}).get("__name__") == TARGET
            and item.get("metric", {}).get("node_id") == remote_node_id
            for item in result
        )

    assert wait_for(query_ready, timeout_seconds=30, step_seconds=1.0), last_query


def test_inmemory_metrics_prometheus_query_supports_fanout_aggregation(ydb_cluster):
    if len(ydb_cluster.nodes) < 2:
        pytest.skip("requires at least two nodes")

    base_url = f"http://localhost:{ydb_cluster.nodes[1].mon_port}"
    last_query = None

    def query_ready():
        nonlocal last_query
        data = get_prometheus_json(
            base_url,
            "/query",
            params={
                "query": f"count by (node_id) ({POOL_TARGET})",
                "time": int(time.time()),
            },
        )
        if not data:
            return False

        last_query = data
        logger.info("prometheus fanout aggregation query result: %s", last_query)
        node_ids = {
            item.get("metric", {}).get("node_id")
            for item in last_query.get("result", [])
            if item.get("metric", {}).get("node_id")
        }
        return last_query.get("resultType") == "vector" and len(node_ids) >= 2

    assert wait_for(query_ready, timeout_seconds=30, step_seconds=1.0), last_query


def test_inmemory_metrics_prometheus_query_rejects_fanout_last_over_time(ydb_cluster):
    if len(ydb_cluster.nodes) < 2:
        pytest.skip("requires at least two nodes")

    base_url = f"http://localhost:{ydb_cluster.nodes[1].mon_port}"
    response = requests.get(
        f"{base_url}{PROMETHEUS_API_PREFIX}/query",
        params={
            "query": f"last_over_time({TARGET}[5m])",
            "time": int(time.time()),
        },
        timeout=10,
    )

    assert_that(response.status_code, equal_to(400))
    payload = response.json()
    assert_that(payload.get("status"), equal_to("error"))
    assert_that(payload.get("errorType"), equal_to("not_implemented"))


def test_inmemory_metrics_prometheus_node_id_values_fanout(ydb_cluster):
    if len(ydb_cluster.nodes) < 2:
        pytest.skip("requires at least two nodes")

    source_base_url = f"http://localhost:{ydb_cluster.nodes[1].mon_port}"
    target_base_url = f"http://localhost:{ydb_cluster.nodes[2].mon_port}"
    target = wait_for_target(
        target_base_url,
        lambda metric: metric.startswith(f'{TARGET}{{node_id="'),
    )
    remote_node_id = extract_label(target, "node_id")

    last_node_ids = None

    def node_ids_ready():
        nonlocal last_node_ids
        data = get_prometheus_json(source_base_url, "/label/node_id/values")
        if not data:
            return False

        last_node_ids = data
        logger.info("prometheus multi-node node_id labels: %s", last_node_ids)
        return remote_node_id in last_node_ids

    assert wait_for(node_ids_ready, timeout_seconds=30, step_seconds=1.0), last_node_ids


def test_inmemory_metrics_prometheus_series_fanout_without_exact_node_id(ydb_cluster):
    if len(ydb_cluster.nodes) < 2:
        pytest.skip("requires at least two nodes")

    base_url = f"http://localhost:{ydb_cluster.nodes[1].mon_port}"
    last_series = None

    def series_ready():
        nonlocal last_series
        data = get_prometheus_json(
            base_url,
            "/series",
            params={"match[]": TARGET},
        )
        if not data:
            return False

        last_series = data
        logger.info("prometheus multi-node series: %s", last_series)
        node_ids = {
            series.get("node_id")
            for series in last_series
            if series.get("__name__") == TARGET and series.get("node_id")
        }
        return len(node_ids) >= 2

    assert wait_for(series_ready, timeout_seconds=30, step_seconds=1.0), last_series


def test_inmemory_metrics_prometheus_query_range_fanout_without_exact_node_id(ydb_cluster):
    if len(ydb_cluster.nodes) < 2:
        pytest.skip("requires at least two nodes")

    base_url = f"http://localhost:{ydb_cluster.nodes[1].mon_port}"
    last_query_range = None
    end = int(time.time())
    start = end - 300

    def query_range_ready():
        nonlocal last_query_range
        data = get_prometheus_json(
            base_url,
            "/query_range",
            params={
                "query": TARGET,
                "start": start,
                "end": end,
                "step": 30,
            },
        )
        if not data:
            return False

        last_query_range = data
        logger.info("prometheus multi-node query_range result: %s", last_query_range)
        node_ids = {
            item.get("metric", {}).get("node_id")
            for item in last_query_range.get("result", [])
            if item.get("metric", {}).get("__name__") == TARGET and item.get("values")
        }
        return len(node_ids) >= 2

    assert wait_for(query_range_ready, timeout_seconds=30, step_seconds=1.0), last_query_range


def test_inmemory_metrics_prometheus_query_range_supports_fanout_aggregation(ydb_cluster):
    if len(ydb_cluster.nodes) < 2:
        pytest.skip("requires at least two nodes")

    base_url = f"http://localhost:{ydb_cluster.nodes[1].mon_port}"
    last_query_range = None
    end = int(time.time())
    start = end - 300

    def query_range_ready():
        nonlocal last_query_range
        data = get_prometheus_json(
            base_url,
            "/query_range",
            params={
                "query": f"count by (node_id) ({POOL_TARGET})",
                "start": start,
                "end": end,
                "step": 30,
            },
        )
        if not data:
            return False

        last_query_range = data
        logger.info("prometheus fanout aggregation query_range result: %s", last_query_range)
        node_ids = {
            item.get("metric", {}).get("node_id")
            for item in last_query_range.get("result", [])
            if item.get("metric", {}).get("node_id") and item.get("values")
        }
        return last_query_range.get("resultType") == "matrix" and len(node_ids) >= 2

    assert wait_for(query_range_ready, timeout_seconds=30, step_seconds=1.0), last_query_range
