"""Tests for measured-test labels and evlog matching."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _paths import DASHBOARD, add_product_paths

add_product_paths(DASHBOARD)

from resources_loader import align_series, build_measured_layers, match_measured_to_runs, measured_test_label


class MeasuredLabelTest(unittest.TestCase):
    def test_label_includes_chunk(self):
        self.assertEqual(
            measured_test_label({"suite": "ydb/core/foo", "chunk": 3}),
            "ydb/core/foo [3]",
        )
        self.assertEqual(measured_test_label({"suite": "unittest"}), "unittest")

    def test_matches_overlapping_evlog_run(self):
        grouped = {
            "ya-test-1": {
                "suite": "unittest",
                "chunk": None,
                "label": "unittest",
                "xs": [100.0, 130.0],
            },
            "ya-test-2": {
                "suite": "unittest",
                "chunk": None,
                "label": "unittest",
                "xs": [140.0, 155.0],
            },
        }
        runs = [
            {
                "suite_path": "ydb/core/foo",
                "chunk": 0,
                "start_sec": 98.0,
                "end_sec": 132.0,
            },
            {
                "suite_path": "ydb/core/foo",
                "chunk": 1,
                "start_sec": 138.0,
                "end_sec": 160.0,
            },
        ]
        match_measured_to_runs(grouped, runs)
        self.assertEqual(grouped["ya-test-1"]["label"], "ydb/core/foo [0]")
        self.assertEqual(grouped["ya-test-2"]["chunk"], 1)
        self.assertEqual(grouped["ya-test-2"]["suite"], "ydb/core/foo")

    def test_align_is_zero_outside_window(self):
        self.assertEqual(align_series([1, 2, 3, 4], [2, 3], [10, 20]), [0.0, 10.0, 20.0, 0.0])

    def test_layers_share_axis(self):
        grouped = {
            "a": {
                "label": "s [0]",
                "xs": [1.0, 2.0],
                "cpu_cores": [1.0, 1.0],
                "ram_gb": [0.2, 0.3],
                "disk_write_mbps": [4.0, 0.0],
                "disk_read_mbps": [0.0, 1.0],
            },
            "b": {
                "label": "s [1]",
                "xs": [2.0, 3.0],
                "cpu_cores": [0.5, 0.5],
                "ram_gb": [0.1, 0.1],
                "disk_write_mbps": [8.0, 8.0],
                "disk_read_mbps": [0.0, 0.0],
            },
        }
        layers = build_measured_layers(grouped)
        self.assertEqual(layers["xs_evlog_sec"], [1.0, 2.0, 3.0])
        self.assertEqual(layers["cpu_tracks"]["s [0]"], [1.0, 1.0, 0.0])
        self.assertEqual(layers["write_tracks"]["s [1]"], [0.0, 8.0, 8.0])


if __name__ == "__main__":
    unittest.main()
