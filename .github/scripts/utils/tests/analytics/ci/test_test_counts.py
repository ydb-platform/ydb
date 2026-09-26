#!/usr/bin/env python3

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "analytics"))

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

    def test_enrich_report_writes_tests_on_the_attempt(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            metrics = Path(tmp) / "ci_metrics.jsonl"
            report.write_text(
                json.dumps({"results": [{"status": "PASSED"}, {"status": "MUTE"}, {"status": "SKIPPED"}]}),
                encoding="utf-8",
            )
            path = str(metrics)
            self.assertEqual(
                main(["start", "ya_make_try_1", "--file", path, "--source", "ya_phase", "--label", "ya_attempt=1"]),
                0,
            )
            self.assertEqual(main(["end", "ya_make_try_1", "--file", path, "--conclusion", "success"]), 0)
            self.assertEqual(
                main(["enrich", "ya_make_try_1", "--file", path, "--label", "ya_attempt=1", "--report", str(report)]),
                0,
            )
            rows = [json.loads(line) for line in metrics.read_text(encoding="utf-8").splitlines() if line.strip()]
            self.assertEqual([row["name"] for row in rows], ["ya_make_try_1"])
            tests = rows[0]["labels"]["tests"]
            self.assertEqual(tests["passed"], 1)
            self.assertEqual(tests["muted"], 1)
            self.assertEqual(tests["skipped"], 1)
            self.assertEqual(tests["failed"], 0)
            self.assertEqual(tests["total"], 3)


if __name__ == "__main__":
    unittest.main()
