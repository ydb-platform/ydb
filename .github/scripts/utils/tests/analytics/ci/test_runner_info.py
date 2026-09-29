#!/usr/bin/env python3
"""Tests for runner cpu/ram/disk cache and usage."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "analytics"))

import os
import tempfile
import unittest

from github_actions.runner_info import (
    INVENTORY_LABEL,
    USAGE_LABEL,
    apply_runner_labels,
    collect_inventory,
    collect_usage,
    load_or_collect_inventory,
    pop_runner_options,
    runner_cache_path,
)


class PopRunnerOptionsTest(unittest.TestCase):
    def test_pops_flags_and_leaves_other_keys(self):
        data = {"runner": True, "usage": "yes", "cache_mode": "none"}
        runner, usage = pop_runner_options(data)
        self.assertTrue(runner)
        self.assertTrue(usage)
        self.assertEqual(data, {"cache_mode": "none"})
        self.assertEqual(pop_runner_options({}), (False, False))
        self.assertEqual(pop_runner_options(None), (False, False))


class RunnerCacheTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.saved = {
            key: os.environ.get(key) for key in ("CI_RUNNER_INFO_FILE", "RUNNER_TEMP", "ANALYTICS_FILE")
        }
        os.environ.pop("CI_RUNNER_INFO_FILE", None)
        os.environ.pop("RUNNER_TEMP", None)
        os.environ.pop("ANALYTICS_FILE", None)

    def tearDown(self):
        for key, value in self.saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_cache_path_falls_back_in_order(self):
        explicit = os.path.join(self.tmp.name, "custom.json")
        runner_temp = os.path.join(self.tmp.name, "runner")
        metrics = os.path.join(self.tmp.name, "ci_metrics.jsonl")
        os.environ["CI_RUNNER_INFO_FILE"] = explicit
        os.environ["RUNNER_TEMP"] = runner_temp
        self.assertEqual(runner_cache_path(metrics), explicit)
        os.environ.pop("CI_RUNNER_INFO_FILE")
        self.assertEqual(runner_cache_path(metrics), os.path.join(runner_temp, "ci_runner_info.json"))
        os.environ.pop("RUNNER_TEMP")
        self.assertEqual(runner_cache_path(metrics), f"{metrics}.runner.json")

    def test_collect_once_then_reuse(self):
        cache = os.path.join(self.tmp.name, "inv.json")
        os.environ["CI_RUNNER_INFO_FILE"] = cache
        calls = {"n": 0}

        def collect():
            calls["n"] += 1
            return {"cpu_count": 4, "mem_total_bytes": 8, "disk_total_bytes": 16}

        first = load_or_collect_inventory(collect=collect)
        second = load_or_collect_inventory(collect=collect)
        self.assertEqual(calls["n"], 1)
        self.assertEqual(first, second)
        self.assertTrue(os.path.isfile(cache))

    def test_empty_collect_does_not_write_cache(self):
        cache = os.path.join(self.tmp.name, "empty.json")
        os.environ["CI_RUNNER_INFO_FILE"] = cache
        self.assertEqual(load_or_collect_inventory(collect=lambda: {}), {})
        self.assertFalse(os.path.exists(cache))

    def test_apply_inventory_setdefault_and_usage_overwrite(self):
        os.environ["CI_RUNNER_INFO_FILE"] = os.path.join(self.tmp.name, "inv.json")
        labels = {INVENTORY_LABEL: {"cpu_count": 1}}
        apply_runner_labels(
            labels,
            runner=True,
            usage=True,
            collect_inventory_fn=lambda: {"cpu_count": 99},
            collect_usage_fn=lambda: {"cpu_pct": 3.5},
        )
        self.assertEqual(labels[INVENTORY_LABEL]["cpu_count"], 1)
        self.assertEqual(labels[USAGE_LABEL]["cpu_pct"], 3.5)
        apply_runner_labels(labels, usage=True, collect_usage_fn=lambda: {"cpu_pct": 9.0})
        self.assertEqual(labels[USAGE_LABEL]["cpu_pct"], 9.0)


class LiveProcTest(unittest.TestCase):
    def test_inventory_has_totals(self):
        if not os.path.isfile("/proc/stat"):
            self.skipTest("not linux")
        inv = collect_inventory()
        self.assertGreater(inv["cpu_count"], 0)
        self.assertGreater(inv["mem_total_bytes"], 0)
        self.assertGreater(inv["disk_total_bytes"], 0)
        self.assertNotIn("disks", inv)

    def test_usage_is_fresh_snapshot(self):
        if not os.path.isfile("/proc/stat"):
            self.skipTest("not linux")
        snap = collect_usage()
        self.assertIn("cpu_pct", snap)
        self.assertIn("mem_used_bytes", snap)
        self.assertIn("disk_used_bytes", snap)
        self.assertGreaterEqual(snap["cpu_pct"], 0.0)
        self.assertGreaterEqual(snap["mem_used_bytes"], 0)
        self.assertGreaterEqual(snap["disk_used_bytes"], 0)


if __name__ == "__main__":
    unittest.main()
