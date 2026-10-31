"""Host /proc samples in the dashboard header, separate from the test model."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _paths import TEST_METRICS, add_product_paths

add_product_paths(TEST_METRICS)

from dashboard_html_payload import _build_headline_stats


class HeadlineHostStatsTest(unittest.TestCase):
    def test_host_samples_are_separate_from_test_tracks(self):
        stats = _build_headline_stats(
            cpu_tracks_suite={"s": [1.0, 2.0]},
            ram_tracks_suite={"s": [0.5]},
            ys_active=[1.0],
            tests_tracks_suite={"s": [1.0]},
            runs=[{"suite_path": "s", "chunk": 0, "start_us": 1_000_000, "end_us": 2_000_000}],
            tests_per_suite={"s": 1},
            issues_summary=None,
            suite_chunk_issues_summary=None,
            resources_overlay={
                "cpu_total_cores": [10.0, 20.0, 30.0, 40.0, 50.0],
                "ram_gb": [100.0, 110.0, 120.0, 130.0, 200.0],
                "disk_read_mb": [1.0, 2.0, 3.0, 4.0, 100.0],
                "disk_write_mb": [0.0, 1.0, 1.0, 1.0, 9.0],
            },
        )
        self.assertEqual(stats["cpu"]["max"], 2.0)
        self.assertEqual(stats["cpu"]["avg"], 1.5)
        self.assertEqual(stats["cpu_host"]["max"], 50.0)
        self.assertEqual(stats["cpu_host"]["median"], 30.0)
        self.assertEqual(stats["cpu_host"]["avg"], 30.0)
        self.assertAlmostEqual(stats["cpu_host"]["p95"], 48.0)
        self.assertNotIn("p90", stats["cpu_host"])
        self.assertEqual(stats["ram_host"]["max"], 200.0)
        self.assertEqual(stats["disk_read_host"]["max"], 100.0)
        self.assertEqual(stats["disk_write_host"]["samples"], 5.0)

    def test_test_cgroup_samples_are_summarized_separately(self):
        stats = _build_headline_stats(
            cpu_tracks_suite={},
            ram_tracks_suite={},
            ys_active=[],
            tests_tracks_suite={},
            runs=[],
            tests_per_suite=None,
            issues_summary=None,
            suite_chunk_issues_summary=None,
            resources_overlay={"measured_layers": {
                "cpu_tracks": {"a": [1.0, 2.0, 0.0], "b": [0.0, 3.0, 0.0]},
                "ram_tracks": {"a": [4.0, 5.0, 0.0], "b": [0.0, 6.0, 0.0]},
                "read_tracks": {"a": [7.0, 8.0, 0.0]},
                "write_tracks": {"a": [9.0, 10.0, 0.0]},
            }},
        )
        self.assertEqual(stats["cpu_cgroup"]["max"], 5.0)
        self.assertEqual(stats["cpu_cgroup"]["avg"], 2.0)
        self.assertEqual(stats["ram_cgroup"]["max"], 11.0)
        self.assertEqual(stats["disk_read_cgroup"]["max"], 8.0)
        self.assertEqual(stats["disk_write_cgroup"]["max"], 10.0)
        self.assertEqual(stats["active_cgroups"]["max"], 2.0)
        self.assertEqual(stats["active_cgroups"]["avg"], 1.0)

    def test_missing_monitor_omits_host_keys(self):
        stats = _build_headline_stats(
            cpu_tracks_suite={},
            ram_tracks_suite={},
            ys_active=[],
            tests_tracks_suite={},
            runs=[],
            tests_per_suite=None,
            issues_summary=None,
            suite_chunk_issues_summary=None,
            resources_overlay=None,
        )
        self.assertNotIn("cpu_host", stats)
        self.assertNotIn("disk_read_host", stats)

    def test_optional_host_contention_signals_are_summarized(self):
        stats = _build_headline_stats(
            cpu_tracks_suite={},
            ram_tracks_suite={},
            ys_active=[],
            tests_tracks_suite={},
            runs=[],
            tests_per_suite=None,
            issues_summary=None,
            suite_chunk_issues_summary=None,
            resources_overlay={"host_resource_signals": {
                "cpu_iowait_pct": [None, 5.0, 10.0],
                "disk_io_in_progress": [0.0, 2.0, 1.0],
            }},
        )
        self.assertEqual(stats["cpu_iowait_pct"]["max"], 10.0)
        self.assertEqual(stats["disk_io_in_progress"]["max"], 2.0)


if __name__ == "__main__":
    unittest.main()
