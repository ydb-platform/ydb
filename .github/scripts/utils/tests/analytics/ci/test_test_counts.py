#!/usr/bin/env python3

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from _paths import ANALYTICS, add_product_paths

add_product_paths(ANALYTICS)

from github_actions.ci_metrics import main
from github_actions.test_counts import count_report_tests


class CountReportTests(unittest.TestCase):
    def test_counts_statuses_after_mute(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "report.json"
            path.write_text(
                json.dumps(
                    {
                        "results": [
                            {"status": "PASSED"},
                            {"status": "OK"},
                            {"status": "FAILED"},
                            {"status": "ERROR"},
                            {"status": "SKIPPED"},
                            {"status": "MUTE"},
                            {"status": "MUTE"},
                            {"status": "CONFIGURE"},
                        ]
                    }
                ),
                encoding="utf-8",
            )
            self.assertEqual(
                count_report_tests(str(path)),
                {
                    "passed": 2,
                    "failed": 1,
                    "errors": 1,
                    "skipped": 1,
                    "muted": 2,
                    "total": 7,
                },
            )

    def test_track_tests_cli_writes_count_events(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            metrics = Path(tmp) / "ci_metrics.jsonl"
            report.write_text(
                json.dumps({"results": [{"status": "PASSED"}, {"status": "MUTE"}, {"status": "SKIPPED"}]}),
                encoding="utf-8",
            )
            self.assertEqual(
                main(["track-tests", "--report", str(report), "--file", str(metrics), "--source", "ya_phase"]),
                0,
            )
            rows = [json.loads(line) for line in metrics.read_text(encoding="utf-8").splitlines() if line.strip()]
            by_name = {row["name"]: row for row in rows}
            self.assertEqual(by_name["tests_passed"]["value"], 1)
            self.assertEqual(by_name["tests_muted"]["value"], 1)
            self.assertEqual(by_name["tests_skipped"]["value"], 1)
            self.assertEqual(by_name["tests_total"]["value"], 3)
            self.assertEqual(by_name["tests_failed"]["kind"], "count")


if __name__ == "__main__":
    unittest.main()
