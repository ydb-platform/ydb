"""Tests for time-trace parsing and aggregation."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parent
_REPO = HERE.parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from ydb.tools.include_sanitizer.timing.parse import parse_trace  # noqa: E402
from ydb.tools.include_sanitizer.timing.aggregate import (  # noqa: E402
    collect_traces,
    write_reports,
)


def _trace(events):
    return {"traceEvents": events, "beginningOfTime": 0}


class ParseTraceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="ydb-tt-"))

    def test_parse_basic(self) -> None:
        p = self.tmp / "x.json"
        p.write_text(json.dumps(_trace([
            {"ph": "X", "name": "ExecuteCompiler", "dur": 1000000},
            {"ph": "X", "name": "Total Frontend", "dur": 700000},
            {"ph": "X", "name": "Total Backend", "dur": 300000},
            {"ph": "X", "name": "Source", "dur": 400000,
             "args": {"detail": "/w/ydb/core/base/appdata.h"}},
            {"ph": "X", "name": "Source", "dur": 100000,
             "args": {"detail": "/w/ydb/core/base/defs.h"}},
            {"ph": "X", "name": "InstantiateClass", "dur": 50000,
             "args": {"detail": "THashMap<TString,int>"}},
            {"ph": "M", "name": "process_name", "args": {"name": "clang"}},
        ])), encoding="utf-8")
        t = parse_trace(p, tu_name="ydb/core/base/appdata.cpp")
        self.assertIsNotNone(t)
        self.assertEqual(t.execute_us, 1000000)
        self.assertEqual(t.frontend_us, 700000)
        self.assertEqual(t.backend_us, 300000)
        self.assertEqual(t.source_us["/w/ydb/core/base/appdata.h"], 400000)
        self.assertEqual(t.template_us["THashMap<TString,int>"], 50000)

    def test_location_fallback_file_attribution(self) -> None:
        """No 'Source' events, but location-tagged parse events -> file_us."""
        p = self.tmp / "z.json"
        p.write_text(json.dumps(_trace([
            {"ph": "X", "name": "ExecuteCompiler", "dur": 800000},
            {"ph": "X", "name": "Total Frontend", "dur": 750000},
            {"ph": "X", "name": "ParseDeclarationOrFunctionDefinition", "dur": 120000,
             "args": {"detail": "/w/ydb/core/base/appdata.h:42:1"}},
            {"ph": "X", "name": "ParseClass", "dur": 30000,
             "args": {"detail": "/w/ydb/core/base/defs.h:10:1"}},
            # a template instantiation whose detail is a TYPE, not a location:
            {"ph": "X", "name": "InstantiateClass", "dur": 5000,
             "args": {"detail": "std::map<int, int>"}},
        ])), encoding="utf-8")
        t = parse_trace(p)
        self.assertEqual(t.source_us, {})
        self.assertEqual(t.file_us.get("/w/ydb/core/base/appdata.h"), 120000)
        self.assertEqual(t.file_us.get("/w/ydb/core/base/defs.h"), 30000)
        # The type-detail instantiation must NOT be mistaken for a file.
        self.assertNotIn("std::map<int, int>", t.file_us)

    def test_location_parser_handles_macro_spelling(self) -> None:
        from ydb.tools.include_sanitizer.timing.parse import _file_from_location
        # bare
        self.assertEqual(
            _file_from_location("/w/ydb/x.h:15:3"), "/w/ydb/x.h")
        # angle range
        self.assertEqual(
            _file_from_location("</w/lib/ordering.h:25:48, col:49>"),
            "/w/lib/ordering.h")
        # macro Spelling suffix must be stripped
        self.assertEqual(
            _file_from_location(
                "/w/lib/grapheme.h:1633:15 <Spelling=/w/lib/__config:9:1>"),
            "/w/lib/grapheme.h")
        # non-locations rejected
        self.assertIsNone(_file_from_location("std::enable_if<true>"))
        self.assertIsNone(_file_from_location("Aws::S3::Model::Foo"))

    def test_frontend_fallback_to_sum(self) -> None:
        p = self.tmp / "y.json"
        p.write_text(json.dumps(_trace([
            {"ph": "X", "name": "ExecuteCompiler", "dur": 900000},
            {"ph": "X", "name": "Frontend", "dur": 300000},
            {"ph": "X", "name": "Frontend", "dur": 300000},
            {"ph": "X", "name": "Backend", "dur": 200000},
        ])), encoding="utf-8")
        t = parse_trace(p)
        self.assertEqual(t.frontend_us, 600000)
        self.assertEqual(t.backend_us, 200000)


class AggregateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="ydb-tt-agg-"))

    def test_collect_and_report(self) -> None:
        tt = self.tmp / "traces"
        tt.mkdir()
        # Two TUs both including appdata.h heavily.
        for i, base_us in enumerate((400000, 600000)):
            (tt / f"tu{i}.json").write_text(json.dumps(_trace([
                {"ph": "X", "name": "ExecuteCompiler", "dur": base_us + 200000},
                {"ph": "X", "name": "Total Frontend", "dur": base_us},
                {"ph": "X", "name": "Total Backend", "dur": 200000},
                {"ph": "X", "name": "Source", "dur": base_us,
                 "args": {"detail": "/w/ydb/core/base/appdata.h"}},
            ])), encoding="utf-8")
            (tt / f"tu{i}.src").write_text(f"/w/ydb/core/base/tu{i}.cpp", encoding="utf-8")

        agg = collect_traces(tt)
        self.assertEqual(len(agg.tus), 2)
        # appdata.h cumulative = 400000 + 600000
        self.assertEqual(agg.header_us["/w/ydb/core/base/appdata.h"], 1000000)
        self.assertEqual(agg.header_tus["/w/ydb/core/base/appdata.h"], 2)
        self.assertEqual(agg.total_frontend, 1000000)
        self.assertEqual(agg.total_backend, 400000)

        out = self.tmp / "report"
        write_reports(agg, out, top=10, trace_dir=tt)
        for name in ("per_tu.csv", "hot_headers.csv", "hot_templates.csv",
                     "summary.md", "event_histogram.csv"):
            self.assertTrue((out / name).exists(), f"missing {name}")
        summary = (out / "summary.md").read_text(encoding="utf-8")
        self.assertIn("appdata.h", summary)
        self.assertIn("frontend", summary.lower())


class ParallelAndCacheTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="ydb-tt-par-"))
        self.tt = self.tmp / "traces"
        self.tt.mkdir()
        for i in range(12):
            (self.tt / f"tu{i}.json").write_text(json.dumps(_trace([
                {"ph": "X", "name": "ExecuteCompiler", "dur": 100000 + i},
                {"ph": "X", "name": "Total Frontend", "dur": 90000},
                {"ph": "X", "name": "Total Backend", "dur": 10000},
                {"ph": "X", "name": "Source", "dur": 50000,
                 "args": {"detail": "/w/common.h"}},
                {"ph": "X", "name": "InstantiateClass", "dur": 1000,
                 "args": {"detail": "std::vector<int>"}},
            ])), encoding="utf-8")

    def test_parallel_matches_and_caches(self) -> None:
        # jobs=1 and jobs=4 must agree.
        a1 = collect_traces(self.tt, jobs=1, use_cache=False)
        a4 = collect_traces(self.tt, jobs=4, use_cache=False)
        self.assertEqual(len(a1.tus), 12)
        self.assertEqual(a1.header_us["/w/common.h"], a4.header_us["/w/common.h"])
        self.assertEqual(a1.header_us["/w/common.h"], 12 * 50000)
        self.assertEqual(a1.total_frontend, a4.total_frontend)

        # Cache: first call writes it, second loads it (identical result).
        cache = self.tmp / "agg.json"
        b1 = collect_traces(self.tt, jobs=2, use_cache=True, cache_path=cache)
        self.assertTrue(cache.exists())
        b2 = collect_traces(self.tt, jobs=2, use_cache=True, cache_path=cache)
        self.assertEqual(b1.to_dict(), b2.to_dict())
        self.assertEqual(b2.header_us["/w/common.h"], 12 * 50000)


if __name__ == "__main__":
    unittest.main()
