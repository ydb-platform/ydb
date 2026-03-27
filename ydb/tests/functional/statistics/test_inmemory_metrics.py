#!/usr/bin/env python

import logging

import requests

from hamcrest import assert_that, equal_to, greater_than

from ydb.tests.library.common.wait_for import wait_for


logger = logging.getLogger(__name__)

TARGET = "harmonizer.max_used_cpu_x1e6"


def test_inmemory_metrics_are_exposed(ydb_cluster):
    mon_port = ydb_cluster.nodes[1].mon_port
    base_url = f"http://localhost:{mon_port}"

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
                f"{base_url}/viewer/inmemory_metrics",
                params={
                    "target": TARGET,
                    "format": "graphite",
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
    assert_that(datapoints[-1][1], greater_than(0))

