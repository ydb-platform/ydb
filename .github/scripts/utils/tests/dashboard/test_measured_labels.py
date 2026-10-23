"""Tests for measured-test labels and evlog matching."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _paths import DASHBOARD, add_product_paths

add_product_paths(DASHBOARD)

from resources_loader import match_measured_to_runs, measured_test_label


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


if __name__ == "__main__":
    unittest.main()
