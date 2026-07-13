"""Unit tests for the per-file compile-time benchmark helpers.

These cover the pure logic (argv shaping, trace->file mapping, baseline
round-trip) without invoking ya or a compiler.
"""

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

from ydb.tools.include_sanitizer.bench import run as bench  # noqa: E402
from ydb.tools.include_sanitizer.common import REPO_ROOT  # noqa: E402


class InjectJ1Test(unittest.TestCase):
    def test_adds_j1_after_make(self) -> None:
        out = bench._inject_j1(["./ya", "make", "ydb/apps/ydbd"])
        self.assertIn("-j1", out)
        self.assertEqual(out.index("make") + 1, out.index("-j1"))

    def test_respects_existing_jobs(self) -> None:
        for existing in (["-j", "8"], ["-j8"], ["--threads", "8"]):
            argv = ["./ya", "make", "ydb/apps/ydbd"] + existing
            out = bench._inject_j1(list(argv))
            self.assertEqual(out, argv)
            self.assertNotIn("-j1", out)


class CollectTest(unittest.TestCase):
    def test_maps_trace_to_target_by_src_sidecar(self) -> None:
        tmp = Path(tempfile.mkdtemp(prefix="ydb-bench-"))
        try:
            # A trace + its .src sidecar (as timetrace_flags writes them).
            # Build the path under the SAME repo root that _collect uses to
            # normalize it (common.REPO_ROOT), which in a hermetic ya test
            # sandbox is not the same as this file's HERE.parents[3].
            src_abs = str(Path(REPO_ROOT) / "ydb/core/foo/bar.cpp")
            (tmp / "abc.src").write_text(src_abs, encoding="utf-8")
            trace = {
                "traceEvents": [
                    {"ph": "X", "name": "ExecuteCompiler", "dur": 1234567},
                    {"ph": "X", "name": "Total Frontend", "dur": 1000000},
                ]
            }
            (tmp / "abc.json").write_text(json.dumps(trace), encoding="utf-8")

            got = bench._collect(tmp, {"ydb/core/foo/bar.cpp"})
            self.assertEqual(got, {"ydb/core/foo/bar.cpp": 1234567})

            # A non-target trace is ignored.
            got2 = bench._collect(tmp, {"ydb/other/x.cpp"})
            self.assertEqual(got2, {})
        finally:
            import shutil
            shutil.rmtree(tmp, ignore_errors=True)


class BaselineRoundTripTest(unittest.TestCase):
    def test_compare_reads_saved_baseline(self) -> None:
        # _print_comparison must not raise on a well-formed baseline and
        # overlapping current results.
        tmp = Path(tempfile.mkdtemp(prefix="ydb-benchcmp-"))
        try:
            import ydb.tools.include_sanitizer.bench.run as m
            orig = m._bench_dir
            m._bench_dir = lambda: tmp  # type: ignore
            try:
                (tmp / "before.json").write_text(json.dumps({
                    "results": {"ydb/a.cpp": {"min_us": 2000000}}
                }), encoding="utf-8")
                cur = {"ydb/a.cpp": {"min_us": 1500000, "median_us": 1500000,
                                     "runs_us": [1500000]}}
                m._print_comparison("before", cur)  # should not raise
            finally:
                m._bench_dir = orig  # type: ignore
        finally:
            import shutil
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
