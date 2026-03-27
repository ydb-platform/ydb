#!/usr/bin/env python

import logging

import requests

from hamcrest import assert_that, equal_to, greater_than

from ydb.tests.library.common.wait_for import wait_for


logger = logging.getLogger(__name__)

TARGET = "harmonizer.max_used_cpu_x1e6"
AWAKENING_TARGET = "harmonizer.avg_awakening_time_us"
POOL_TARGET = 'harmonizer.pool.avg_used_cpu_x1e6{pool="IC",pool_id="4"}'


def test_inmemory_metrics_are_exposed(ydb_cluster):
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f"http://localhost:{mon_port}"

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
        return any(item.get("id") == TARGET for item in last_find)

    assert wait_for(graphite_find_ready, timeout_seconds=30, step_seconds=1.0), last_find

    last_targets = []

    def targets_ready():
        nonlocal last_targets
        try:
            response = requests.get(
                f"{base_url}/viewer/inmemory_metrics/targets",
                params={"prefix": "harmonizer"},
                timeout=10,
            )
        except requests.exceptions.RequestException:
            return False

        if response.status_code != 200:
            return False

        last_targets = response.json()
        logger.info("inmemory metric targets: %s", last_targets)
        return TARGET in last_targets

    assert wait_for(targets_ready, timeout_seconds=30, step_seconds=1.0), last_targets

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
    assert_that(last_series[0]["target"], equal_to(TARGET))
    assert_that(len(datapoints), greater_than(0))
    assert_that(datapoints[-1][0], greater_than(0))


def test_inmemory_metrics_render_accepts_labeled_graphite_target(ydb_cluster):
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f"http://localhost:{mon_port}"

    last_series = None

    def series_ready():
        nonlocal last_series
        try:
            response = requests.get(
                f"{base_url}/viewer/inmemory_metrics/render",
                params={"target": POOL_TARGET},
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
            and last_series[0].get("target") == POOL_TARGET
            and bool(last_series[0].get("datapoints"))
        )

    assert wait_for(series_ready, timeout_seconds=30, step_seconds=1.0), last_series

    datapoints = last_series[0]["datapoints"]
    assert_that(last_series[0]["target"], equal_to(POOL_TARGET))
    assert_that(len(datapoints), greater_than(0))


def test_inmemory_metrics_render_supports_graphite_alias_sub_json_format(ydb_cluster):
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f"http://localhost:{mon_port}"
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
            and last_series[0].get("target") == f"{AWAKENING_TARGET} A"
            and bool(last_series[0].get("datapoints"))
        )

    assert wait_for(series_ready, timeout_seconds=30, step_seconds=1.0), last_series

    datapoints = last_series[0]["datapoints"]
    assert_that(last_series[0]["target"], equal_to(f"{AWAKENING_TARGET} A"))
    assert_that(len(datapoints), greater_than(0))
