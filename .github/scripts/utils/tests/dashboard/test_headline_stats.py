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
        self.assertEqual(stats["cpu_host"]["max"], 50.0)
        self.assertEqual(stats["cpu_host"]["median"], 30.0)
        self.assertAlmostEqual(stats["cpu_host"]["p95"], 48.0)
        self.assertNotIn("p90", stats["cpu_host"])
        self.assertEqual(stats["ram_host"]["max"], 200.0)
        self.assertEqual(stats["disk_read_host"]["max"], 100.0)
        self.assertEqual(stats["disk_write_host"]["samples"], 5.0)

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


if __name__ == "__main__":
    unittest.main()
