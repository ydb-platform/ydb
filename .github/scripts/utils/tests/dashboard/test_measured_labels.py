"""Tests for measured-test labels and evlog matching."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _paths import DASHBOARD, add_product_paths

add_product_paths(DASHBOARD)

from resources_loader import (
    align_series,
    build_measured_layers,
    match_measured_to_runs,
    measured_test_label,
    normalize_report_links,
    place_chunk_tests,
)


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

    def test_places_named_tests_end_to_end(self):
        placed = place_chunk_tests(
            [{"name": "Foo", "duration_sec": 8, "status": "PASSED"}, {"name": "Bar", "duration_sec": 12}],
            start_sec=100,
            delay_sec=2,
        )
        self.assertEqual(placed[0]["name"], "Foo")
        self.assertEqual(placed[0]["start_sec"], 102)
        self.assertEqual(placed[0]["end_sec"], 110)
        self.assertEqual(placed[1]["name"], "Bar")
        self.assertEqual(placed[1]["start_sec"], 110)
        self.assertEqual(placed[1]["end_sec"], 122)

    def test_keeps_status_error_and_log_links(self):
        self.assertEqual(
            normalize_report_links({"stderr": ["https://s3/stderr"], "log": "https://s3/log", "empty": []}),
            {"stderr": "https://s3/stderr", "log": "https://s3/log"},
        )
        placed = place_chunk_tests(
            [
                {
                    "name": "Boom",
                    "duration_sec": 4,
                    "status": "FAILED",
                    "error_type": "VERIFY, TIMEOUT",
                    "links": {"stderr": ["https://s3/stderr"], "logsdir": ["https://s3/dir"]},
                }
            ],
            start_sec=10,
        )
        self.assertEqual(placed[0]["status"], "FAILED")
        self.assertEqual(placed[0]["error_type"], "VERIFY, TIMEOUT")
        self.assertEqual(placed[0]["links"]["stderr"], "https://s3/stderr")
        self.assertEqual(placed[0]["links"]["logsdir"], "https://s3/dir")


if __name__ == "__main__":
    unittest.main()
