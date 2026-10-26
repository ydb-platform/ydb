import tempfile
import unittest
import json
from pathlib import Path
from unittest.mock import patch

import resource_usage_report as report
import testowners_utils


def chunk(path="ydb/example", **metrics):
    return {"type": "test", "chunk": True, "path": path, "metrics": metrics}


def assign_owners(suites):
    for suite in suites:
        suite["owners"] = "TEAM:@ydb-platform/core"


class ResourceUsageTest(unittest.TestCase):
    def build(self, results):
        return report.build_usage(results, {"core": "area/core"}, assign_owners)

    def test_only_chunks_contribute_once(self):
        metrics = dict(ru_utime=10, ru_stime=2, ru_inblock=3, ru_oublock=4)
        results = [chunk(**metrics), chunk(**metrics),
                   {"type": "test", "suite": True, "path": "ydb/example", "metrics": metrics},
                   {"type": "test", "path": "ydb/example", "metrics": metrics},
                   {"type": "build", "chunk": True, "path": "ydb/example", "metrics": metrics}]
        data = self.build(results)
        self.assertEqual(len(data["suites"]), 1)
        total = data["totals"]
        self.assertEqual(total["chunks"], 2)
        self.assertEqual(total["cpu_seconds"], 24)
        self.assertEqual(total["read_bytes"], 6 * 512)
        self.assertEqual(total["write_bytes"], 8 * 512)
        self.assertEqual(data["areas"][0]["write_bytes_pct_total"], 100)

    def test_missing_invalid_and_zero_are_distinct(self):
        data = self.build([chunk(ru_utime=0, ru_stime=0, ru_inblock=0, ru_oublock=0),
                           chunk(ru_utime=100, ru_stime="invalid", ru_inblock=-1, ru_oublock=float("nan")),
                           chunk()])
        suite = data["suites"][0]
        self.assertEqual(suite["cpu_seconds"], 0)
        self.assertEqual(suite["cpu_seconds_chunks"], 1)
        self.assertEqual(suite["write_bytes_chunks"], 1)
        self.assertEqual(report.format_usage(suite, "cpu_seconds"), "0.000 †")
        missing = self.build([chunk()])["suites"][0]
        self.assertIsNone(missing["write_bytes"])
        self.assertIsNone(missing["write_bytes_pct_total"])
        self.assertEqual(report.format_usage(missing, "write_bytes"), "—")

    def test_shared_owners_do_not_double_charge(self):
        def shared(suites):
            suites[0]["owners"] = "TEAM:@ydb-platform/core;;TEAM:@ydb-platform/qp;;TEAM:@ydb-platform/core"
            suites[1]["owners"] = "USERNAME:@someone"
        data = report.build_usage([chunk("ydb/a", ru_utime=1, ru_stime=1),
                                   chunk("ydb/b", ru_utime=3, ru_stime=1)],
                                  {"core": "area/core", "qp": "area/queryprocessor"}, shared)
        self.assertEqual({r["area"] for r in data["areas"]},
                         {"unassigned", "area/core + area/queryprocessor"})
        self.assertEqual(sum(r["cpu_seconds"] for r in data["areas"]), 6)
        self.assertAlmostEqual(sum(r["cpu_seconds_pct_total"] for r in data["areas"]), 100)

    def test_testowners_specific_paths_globs_and_case(self):
        with tempfile.TemporaryDirectory() as tmp:
            owners = Path(tmp) / "TESTOWNERS"
            owners.write_text("/ydb/core/tx/schemeshard/ut_login* @ydb-platform/Security\n"
                              "/ydb/core @ydb-platform/core\n")
            with patch.object(testowners_utils, "TESTOWNERS_FILE", str(owners)):
                data = report.build_usage([chunk("ydb/core/tx/schemeshard/ut_login_large"),
                                           chunk("ydb/core/example")], {"security": "area/security"})
        self.assertEqual({r["suite_folder"]: r["area"] for r in data["suites"]},
                         {"ydb/core/tx/schemeshard/ut_login_large": "area/security",
                          "ydb/core/example": "team/core"})

    def test_suite_shares_use_own_area_and_all_suites(self):
        data = self.build([chunk("ydb/a", ru_utime=1, ru_stime=0),
                           chunk("ydb/b", ru_utime=3, ru_stime=0)])
        suite = next(r for r in data["suites"] if r["suite_folder"] == "ydb/a")
        self.assertEqual(suite["cpu_seconds_pct_area"], 25)
        self.assertEqual(suite["cpu_seconds_pct_total"], 25)

    def test_empty_report(self):
        data = self.build([])
        self.assertEqual(data["suites"], [])
        self.assertEqual(data["areas"], [])
        self.assertIsNone(data["totals"]["cpu_seconds"])

    def test_suite_without_chunk_keeps_unknown_usage(self):
        data = self.build([{"type": "test", "suite": True, "path": "ydb/not_started",
                            "metrics": {"ru_utime": 100, "ru_stime": 100}}])
        self.assertEqual(data["totals"]["suites"], 1)
        self.assertEqual(data["totals"]["chunks"], 0)
        self.assertIsNone(data["totals"]["cpu_seconds"])

    def test_export_and_html_escape_suite_names(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "report.json"
            source.write_text(json.dumps({"results": [chunk('ydb/<script>"', ru_utime=2, ru_stime=1)]}))
            report.write_report(source, Path(tmp) / "out")
            html = (Path(tmp) / "out/index.html").read_text()
            self.assertNotIn('ydb/<script>', html)
            self.assertIn('ydb/&lt;script&gt;', html)
            usage = json.loads((Path(tmp) / "out/usage.json").read_text())
            self.assertEqual(usage["totals"]["cpu_seconds"], 3)
            self.assertTrue((Path(tmp) / "out/suites.csv").is_file())


if __name__ == "__main__":
    unittest.main()
