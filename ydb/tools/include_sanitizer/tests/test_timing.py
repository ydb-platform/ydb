"""Tests for time-trace parsing and aggregation."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


_TEST_YDB_ROOT = "ydb"

HERE = Path(__file__).resolve().parent
_REPO = HERE.parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from ydb.tools.include_sanitizer.timing.parse import parse_trace  # noqa: E402
from ydb.tools.include_sanitizer.timing.aggregate import (  # noqa: E402
    collect_traces,
    event_histogram,
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
             "args": {"detail": f'/w/{_TEST_YDB_ROOT}/core/base/appdata.h'}},
            {"ph": "X", "name": "Source", "dur": 100000,
             "args": {"detail": f'/w/{_TEST_YDB_ROOT}/core/base/defs.h'}},
            {"ph": "X", "name": "InstantiateClass", "dur": 50000,
             "args": {"detail": "THashMap<TString,int>"}},
            {"ph": "M", "name": "process_name", "args": {"name": "clang"}},
        ])), encoding="utf-8")
        t = parse_trace(p, tu_name=f'{_TEST_YDB_ROOT}/core/base/appdata.cpp')
        self.assertIsNotNone(t)
        self.assertEqual(t.execute_us, 1000000)
        self.assertEqual(t.frontend_us, 700000)
        self.assertEqual(t.backend_us, 300000)
        self.assertEqual(t.source_us[f'/w/{_TEST_YDB_ROOT}/core/base/appdata.h'], 400000)
        self.assertEqual(t.template_us["THashMap<TString,int>"], 50000)

    def test_location_fallback_file_attribution(self) -> None:
        """No 'Source' events, but location-tagged parse events -> file_us."""
        p = self.tmp / "z.json"
        p.write_text(json.dumps(_trace([
            {"ph": "X", "name": "ExecuteCompiler", "dur": 800000},
            {"ph": "X", "name": "Total Frontend", "dur": 750000},
            {"ph": "X", "name": "ParseDeclarationOrFunctionDefinition", "dur": 120000,
             "args": {"detail": f'/w/{_TEST_YDB_ROOT}/core/base/appdata.h:42:1'}},
            {"ph": "X", "name": "ParseClass", "dur": 30000,
             "args": {"detail": f'/w/{_TEST_YDB_ROOT}/core/base/defs.h:10:1'}},
            # a template instantiation whose detail is a TYPE, not a location:
            {"ph": "X", "name": "InstantiateClass", "dur": 5000,
             "args": {"detail": "std::map<int, int>"}},
        ])), encoding="utf-8")
        t = parse_trace(p)
        self.assertEqual(t.source_us, {})
        self.assertEqual(t.file_us.get(f'/w/{_TEST_YDB_ROOT}/core/base/appdata.h'), 120000)
        self.assertEqual(t.file_us.get(f'/w/{_TEST_YDB_ROOT}/core/base/defs.h'), 30000)
        # The type-detail instantiation must NOT be mistaken for a file.
        self.assertNotIn("std::map<int, int>", t.file_us)

    def test_async_source_events(self) -> None:
        # LLVM emits the child's complete pair before the parent's pair,
        # reusing id=0. End events do not carry the header detail.
        common = {"pid": 1, "tid": 2, "cat": "Source", "id": 0, "name": "Source"}
        events = []
        for header, start, end in (("child.h", 20, 30), ("parent.h", 0, 50),
                                   ("child.h", 60, 80)):
            events.extend([
                dict(common, ph="b", ts=start, args={"detail": header}),
                dict(common, ph="e", ts=end),
            ])
        events.append({"ph": "X", "name": "Total Source", "dur": 70})
        p = self.tmp / "async.json"
        p.write_text(json.dumps(_trace(events)))
        timing = parse_trace(p)
        self.assertEqual(timing.source_us, {"child.h": 30, "parent.h": 50})
        self.assertEqual(timing.totals["Total Source"], 70)
        self.assertEqual(event_histogram(self.tmp)["Source"], (3, 80))

    def test_incomplete_async_events_are_ignored(self) -> None:
        common = {"pid": 1, "tid": 2, "cat": "Source", "id": 0, "name": "Source"}
        p = self.tmp / "incomplete.json"
        p.write_text(json.dumps(_trace([
            dict(common, ph="e", ts=10),
            dict(common, ph="b", ts=30, args={"detail": "negative.h"}),
            dict(common, ph="e", ts=20),
            dict(common, ph="b", ts=40, args={"detail": "unfinished.h"}),
        ])))
        self.assertEqual(parse_trace(p).source_us, {})

    def test_location_parser_handles_macro_spelling(self) -> None:
        from ydb.tools.include_sanitizer.timing.parse import _file_from_location
        # bare
        self.assertEqual(
            _file_from_location(f'/w/{_TEST_YDB_ROOT}/x.h:15:3'), f'/w/{_TEST_YDB_ROOT}/x.h')
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
                 "args": {"detail": f'/w/{_TEST_YDB_ROOT}/core/base/appdata.h'}},
            ])), encoding="utf-8")
            (tt / f"tu{i}.src").write_text(f"/w/ydb/core/base/tu{i}.cpp", encoding="utf-8")

        agg = collect_traces(tt)
        self.assertEqual(len(agg.tus), 2)
        # appdata.h cumulative = 400000 + 600000
        self.assertEqual(agg.header_us[f'/w/{_TEST_YDB_ROOT}/core/base/appdata.h'], 1000000)
        self.assertEqual(agg.header_tus[f'/w/{_TEST_YDB_ROOT}/core/base/appdata.h'], 2)
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

    def test_generated_headers_are_merged_across_build_roots(self) -> None:
        tt = self.tmp / "traces"
        tt.mkdir()
        generated_header = f"{_TEST_YDB_ROOT}/core/protos/config.pb.h"
        source_header = f"{_TEST_YDB_ROOT}/core/base/defs.h"
        for i in range(2):
            header = f"/cache/build_root/run/{i:06x}/{generated_header}"
            (tt / f"tu{i}.json").write_text(json.dumps(_trace([
                {"ph": "X", "name": "Source", "dur": 400000,
                 "args": {"detail": header}},
                {"ph": "X", "name": "Source", "dur": 100000,
                 "args": {"detail": f"/w/{source_header}"}},
                {"ph": "X", "name": "Source", "dur": 10000,
                 "args": {"detail": source_header}},
                {"ph": "X", "name": "ParseClass", "dur": 50000,
                 "args": {"detail": header + ":10:1"}},
            ])), encoding="utf-8")

        agg = collect_traces(tt, repo_root=Path("/w"), jobs=1)
        self.assertEqual(agg.header_us[generated_header], 800000)
        self.assertEqual(agg.header_tus[generated_header], 2)
        self.assertEqual(agg.header_us[source_header], 220000)
        self.assertEqual(agg.header_tus[source_header], 2)
        self.assertEqual(agg.file_us[generated_header], 100000)
        self.assertEqual(agg.file_tus[generated_header], 2)

        # A cache made with path normalization cannot serve a raw-path run.
        raw = collect_traces(tt, jobs=1)
        self.assertNotIn(generated_header, raw.header_us)
        self.assertIn(f"/w/{source_header}", raw.header_us)


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
