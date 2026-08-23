"""Unit tests for the reproducible build-cost measurement.

Pure logic only: counter parsing (including output no tool would be proud
of), argv shaping, the statistics behind the noise-aware comparison, and
the graceful-degradation paths. Nothing here runs ya, clang or perf.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parent
_REPO = HERE.parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from ydb.tools.include_sanitizer.buildbench import parse as bbparse  # noqa: E402
from ydb.tools.include_sanitizer.buildbench import run as bbrun  # noqa: E402
from ydb.tools.include_sanitizer.common import REPO_ROOT  # noqa: E402
from ydb.tools.include_sanitizer.compdb import record_cc  # noqa: E402


class TempDirTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="ydb-buildbench-"))

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)


class ParseProcStatTest(TempDirTest):
    def test_single_line(self) -> None:
        path = self.tmp / "a.pstat"
        path.write_text('"clang++","/build/a.o",297517,246423,77764\n',
                        encoding="utf-8")
        stat = bbparse.parse_procstat(path)
        self.assertIsNotNone(stat)
        self.assertEqual(stat.wall_us, 297517)
        self.assertEqual(stat.user_us, 246423)
        self.assertEqual(stat.peak_rss_kb, 77764)
        self.assertEqual(stat.subprocesses, 1)

    def test_multiple_subprocesses_add_time_and_max_memory(self) -> None:
        path = self.tmp / "a.pstat"
        path.write_text(
            '"clang++","/build/a.o",100,80,1000\n'
            '"clang++","/build/a.o",200,150,3000\n',
            encoding="utf-8")
        stat = bbparse.parse_procstat(path)
        self.assertEqual(stat.wall_us, 300)
        self.assertEqual(stat.user_us, 230)
        self.assertEqual(stat.peak_rss_kb, 3000)
        self.assertEqual(stat.subprocesses, 2)

    def test_malformed_lines_are_skipped(self) -> None:
        path = self.tmp / "a.pstat"
        path.write_text(
            "\n"
            '"clang++","/build/a.o"\n'
            '"clang++","/build/a.o",oops,nope,nah\n'
            '"clang++","/build/a.o",100,80,1000\n',
            encoding="utf-8")
        stat = bbparse.parse_procstat(path)
        self.assertEqual(stat.subprocesses, 1)
        self.assertEqual(stat.user_us, 80)

    def test_missing_or_empty_file_yields_none(self) -> None:
        self.assertIsNone(bbparse.parse_procstat(self.tmp / "nope.pstat"))
        empty = self.tmp / "empty.pstat"
        empty.write_text("", encoding="utf-8")
        self.assertIsNone(bbparse.parse_procstat(empty))


class ParsePerfTest(TempDirTest):
    def test_parses_csv_and_skips_banner(self) -> None:
        path = self.tmp / "a.perf"
        path.write_text(
            "# started on Sat Aug  8 12:00:00 2026\n"
            "\n"
            "1035794631,,instructions:u,262379496,100.00,,\n"
            "262.379496,msec,task-clock:u,262379496,100.00,,\n",
            encoding="utf-8")
        counters = bbparse.parse_perf(path)
        self.assertEqual(counters["instructions:u"], 1035794631.0)
        self.assertAlmostEqual(counters["task-clock:u"], 262.379496)

    def test_uncounted_events_are_dropped(self) -> None:
        path = self.tmp / "a.perf"
        path.write_text(
            "<not counted>,,instructions:u,0,0.00,,\n"
            "<not supported>,,cycles:u,0,0.00,,\n"
            "junk-line-without-commas\n"
            "17,,branches:u,100,100.00,,\n",
            encoding="utf-8")
        counters = bbparse.parse_perf(path)
        self.assertEqual(counters, {"branches:u": 17.0})

    def test_missing_file_is_empty(self) -> None:
        self.assertEqual(bbparse.parse_perf(self.tmp / "nope.perf"), {})

    def test_perf_value_ignores_modifier_suffix(self) -> None:
        counters = {"instructions:u": 5.0}
        self.assertEqual(bbparse.perf_value(counters, "instructions"), 5.0)
        self.assertIsNone(bbparse.perf_value(counters, "cycles"))
        self.assertEqual(bbparse.perf_value({"instructions": 7.0},
                                            "instructions"), 7.0)


class ProbePerfTest(unittest.TestCase):
    def test_missing_binary_degrades_to_none(self) -> None:
        self.assertIsNone(bbparse.probe_perf("/nonexistent/perf"))

    def test_non_perf_binary_degrades_to_none(self) -> None:
        # /bin/sh exists but is not perf: the functional probe must reject
        # it rather than let the caller believe counters are available.
        if not os.path.exists("/bin/sh"):
            self.skipTest("no /bin/sh")
        self.assertIsNone(bbparse.probe_perf("/bin/sh"))


class CollectTuCostsTest(TempDirTest):
    def test_joins_procstat_and_perf_on_shared_digest(self) -> None:
        ps_dir = self.tmp / "pstat"
        perf_dir = self.tmp / "perf"
        ps_dir.mkdir()
        perf_dir.mkdir()
        source = str(Path(REPO_ROOT) / "ydb/core/foo/bar.cpp")
        for d in (ps_dir, perf_dir):
            (d / "deadbeef.src").write_text(source, encoding="utf-8")
        (ps_dir / "deadbeef.pstat").write_text(
            '"clang++","/build/bar.o",500,400,2048\n', encoding="utf-8")
        (perf_dir / "deadbeef.perf").write_text(
            "1000,,instructions:u,500,100.00,,\n", encoding="utf-8")

        costs = bbparse.collect_tu_costs(ps_dir, perf_dir)
        self.assertEqual(list(costs), ["ydb/core/foo/bar.cpp"])
        cost = costs["ydb/core/foo/bar.cpp"]
        self.assertEqual(cost.user_us, 400)
        self.assertEqual(cost.instructions, 1000.0)
        self.assertEqual(cost.compiles, 1)

    def test_same_source_compiled_twice_is_summed(self) -> None:
        ps_dir = self.tmp / "pstat"
        ps_dir.mkdir()
        source = str(Path(REPO_ROOT) / "ydb/core/foo/bar.cpp")
        for digest in ("aaa", "bbb"):
            (ps_dir / f"{digest}.src").write_text(source, encoding="utf-8")
            (ps_dir / f"{digest}.pstat").write_text(
                '"clang++","/build/bar.o",500,400,2048\n', encoding="utf-8")
        costs = bbparse.collect_tu_costs(ps_dir)
        cost = costs["ydb/core/foo/bar.cpp"]
        self.assertEqual(cost.compiles, 2)
        self.assertEqual(cost.user_us, 800)

    def test_missing_counter_files_do_not_break_collection(self) -> None:
        ps_dir = self.tmp / "pstat"
        ps_dir.mkdir()
        (ps_dir / "abc.src").write_text(
            str(Path(REPO_ROOT) / "ydb/a.cpp"), encoding="utf-8")
        costs = bbparse.collect_tu_costs(ps_dir, self.tmp / "absent")
        self.assertEqual(costs["ydb/a.cpp"].user_us, 0)

    def test_clear_artifacts_removes_everything(self) -> None:
        d = self.tmp / "pstat"
        d.mkdir()
        for name in ("a.src", "a.pstat", "b.perf", "c.json"):
            (d / name).write_text("x", encoding="utf-8")
        bbparse.clear_artifacts([d, None, self.tmp / "absent"])
        self.assertEqual(list(d.iterdir()), [])


class ShimFlagTest(TempDirTest):
    """The recorder must file every tier under one shared digest."""

    def test_procstat_and_timetrace_share_the_digest(self) -> None:
        args = ["-c", "foo.cpp", "-o", "foo.o"]
        tt = record_cc.timetrace_flags(args, self.tmp / "tt")
        ps = record_cc.procstat_flags(args, self.tmp / "ps")
        tt_stem = Path(tt[0].split("=", 1)[1]).stem
        ps_stem = Path(ps[0].split("=", 1)[1]).stem
        self.assertEqual(tt_stem, ps_stem)
        self.assertTrue((self.tmp / "ps" / f"{ps_stem}.src").exists())

    def test_no_source_means_no_flags(self) -> None:
        self.assertEqual(record_cc.procstat_flags(["--version"], self.tmp), [])
        self.assertEqual(
            record_cc.perf_wrap(["clang", "--version"], self.tmp,
                                perf_bin=sys.executable),
            ["clang", "--version"])

    def test_perf_wrap_builds_a_stat_invocation(self) -> None:
        cmd = ["/usr/bin/clang++", "-c", "foo.cpp", "-o", "foo.o"]
        out = record_cc.perf_wrap(cmd, self.tmp, perf_bin=sys.executable,
                                  events="instructions:u")
        self.assertEqual(out[0], sys.executable)
        self.assertEqual(out[1], "stat")
        self.assertIn("instructions:u", out)
        self.assertEqual(out[out.index("--") + 1:], cmd)

    def test_perf_wrap_without_perf_is_a_no_op(self) -> None:
        cmd = ["/usr/bin/clang++", "-c", "foo.cpp", "-o", "foo.o"]
        self.assertEqual(
            record_cc.perf_wrap(cmd, self.tmp, perf_bin="/nonexistent/perf"),
            cmd)

    def test_shim_handle_puts_perf_outermost(self) -> None:
        cmd = ["/usr/bin/clang++", "-c", "foo.cpp", "-o", "foo.o"]
        out = record_cc.shim_handle(cmd, None, None,
                                    str(self.tmp / "ps"),
                                    str(self.tmp / "perf"))
        self.assertEqual(out[0], record_cc.DEFAULT_PERF_BIN
                         if os.path.exists(record_cc.DEFAULT_PERF_BIN)
                         else "/usr/bin/clang++")
        joined = " ".join(out)
        self.assertIn("-fproc-stat-report=", joined)
        # The compiler flag must land on the compiler, not on perf.
        stat_flag = next(a for a in out if a.startswith("-fproc-stat-report="))
        self.assertGreater(out.index(stat_flag), out.index("/usr/bin/clang++"))

    def test_shim_handle_without_dirs_leaves_the_command_alone(self) -> None:
        cmd = ["/usr/bin/clang++", "-c", "foo.cpp", "-o", "foo.o"]
        saved = {k: os.environ.pop(k) for k in
                 ("YDB_COMPDB_DIR", "YDB_TIMETRACE_DIR", "YDB_PSTAT_DIR",
                  "YDB_PERF_DIR") if k in os.environ}
        try:
            self.assertEqual(record_cc.shim_handle(list(cmd)), cmd)
        finally:
            os.environ.update(saved)


class ArgvShapingTest(unittest.TestCase):
    def test_inject_jobs_after_make(self) -> None:
        out = bbrun.inject_jobs(["./ya", "make", "ydb/core"], 64)
        self.assertEqual(out, ["./ya", "make", "-j64", "ydb/core"])

    def test_inject_jobs_replaces_existing_selection(self) -> None:
        for existing in (["-j", "8"], ["-j8"], ["--threads", "8"],
                         ["--threads=8"]):
            argv = ["./ya", "make", "ydb/core"] + existing
            out = bbrun.inject_jobs(argv, 32)
            self.assertEqual(out, ["./ya", "make", "-j32", "ydb/core"],
                             msg=f"for {existing}")

    def test_inject_jobs_none_is_a_no_op(self) -> None:
        argv = ["./ya", "make", "-j8", "ydb/core"]
        self.assertEqual(bbrun.inject_jobs(argv, None), argv)

    def test_inject_jobs_on_a_non_ya_command_appends(self) -> None:
        out = bbrun.inject_jobs(["make", "all"], 4)
        self.assertEqual(out, ["make", "all", "-j4"])

    def test_strip_jobs_keeps_unrelated_flags(self) -> None:
        out = bbrun.strip_jobs(["./ya", "make", "-j8", "--rebuild", "-r",
                                "ydb/core"])
        self.assertEqual(out, ["./ya", "make", "--rebuild", "-r", "ydb/core"])

    def test_taskset_prefix_covers_the_requested_cores(self) -> None:
        prefix = bbrun.taskset_prefix(16)
        if not prefix:
            self.skipTest("taskset not installed")
        self.assertEqual(prefix[-2:], ["-c", "0-15"])
        self.assertEqual(bbrun.taskset_prefix(None), [])
        self.assertEqual(bbrun.taskset_prefix(0), [])


class StatisticsTest(unittest.TestCase):
    def test_summarize_median_and_spread(self) -> None:
        stats = bbrun.summarize([10.0, 12.0, 11.0])
        self.assertEqual(stats["median"], 11.0)
        self.assertEqual(stats["min"], 10.0)
        self.assertEqual(stats["max"], 12.0)
        self.assertAlmostEqual(stats["spread_pct"], 2.0 / 11.0 * 100.0)
        self.assertEqual(stats["n"], 3)

    def test_summarize_even_count_averages_the_middle(self) -> None:
        self.assertEqual(bbrun.summarize([10.0, 20.0])["median"], 15.0)

    def test_summarize_of_nothing_is_none(self) -> None:
        self.assertIsNone(bbrun.summarize([]))
        self.assertIsNone(bbrun.summarize([None, None]))

    def test_noise_band_is_the_larger_of_floor_and_observed_spread(self) -> None:
        quiet = {"spread_pct": 0.1}
        loud = {"spread_pct": 9.0}
        self.assertEqual(bbrun.noise_band("sum_tu_instructions", quiet, quiet),
                         0.5)
        self.assertEqual(bbrun.noise_band("sum_tu_instructions", quiet, loud),
                         9.0)

    def test_primary_metric_prefers_instructions(self) -> None:
        self.assertEqual(
            bbrun.pick_primary({"wall_s": {}, "sum_tu_user_s": {},
                                "sum_tu_instructions": {}}),
            "sum_tu_instructions")
        self.assertEqual(bbrun.pick_primary({"wall_s": {}, "sum_tu_user_s": {}}),
                         "sum_tu_user_s")
        self.assertIsNone(bbrun.pick_primary({"cpu_sys_s": {}}))


class DerivedDiagnosticsTest(unittest.TestCase):
    def _point(self, **kwargs) -> bbrun.Point:
        point = bbrun.Point(jobs=kwargs.pop("jobs", 64))
        point.runs.append(bbrun.RunSample(**kwargs))
        return point

    def test_parallel_efficiency(self) -> None:
        point = self._point(jobs=10, wall_s=100.0, sum_tu_user_s=500.0)
        self.assertAlmostEqual(bbrun.parallel_efficiency(point), 0.5)

    def test_parallel_efficiency_needs_a_job_count(self) -> None:
        point = self._point(jobs=None, wall_s=100.0, sum_tu_user_s=500.0)
        self.assertIsNone(bbrun.parallel_efficiency(point))

    def test_non_compile_instructions(self) -> None:
        point = self._point(instructions=1000.0, sum_tu_instructions=750.0)
        absolute, pct = bbrun.non_compile_instructions(point)
        self.assertEqual(absolute, 250.0)
        self.assertAlmostEqual(pct, 25.0)

    def test_non_compile_instructions_without_perf_is_none(self) -> None:
        self.assertIsNone(bbrun.non_compile_instructions(
            self._point(sum_tu_instructions=750.0)))

    def test_fold_tu_costs(self) -> None:
        costs = {
            "a.cpp": bbparse.TuCost("a.cpp", compiles=1, user_us=1_000_000,
                                    wall_us=2_000_000, peak_rss_kb=100,
                                    instructions=10.0),
            "b.cpp": bbparse.TuCost("b.cpp", compiles=2, user_us=500_000,
                                    wall_us=500_000, peak_rss_kb=900,
                                    instructions=5.0),
        }
        sample = bbrun.RunSample()
        bbrun.fold_tu_costs(sample, costs)
        self.assertEqual(sample.tu_count, 2)
        self.assertEqual(sample.compile_count, 3)
        self.assertAlmostEqual(sample.sum_tu_user_s, 1.5)
        self.assertEqual(sample.sum_tu_instructions, 15.0)
        self.assertEqual(sample.max_tu_peak_rss_kb, 900)

    def test_merge_tu_costs_takes_min_time_and_median_instructions(self) -> None:
        reps = [
            {"a.cpp": bbparse.TuCost("a.cpp", compiles=1, user_us=300,
                                     instructions=110.0, peak_rss_kb=10)},
            {"a.cpp": bbparse.TuCost("a.cpp", compiles=1, user_us=100,
                                     instructions=100.0, peak_rss_kb=30)},
            {"a.cpp": bbparse.TuCost("a.cpp", compiles=1, user_us=200,
                                     instructions=105.0, peak_rss_kb=20)},
        ]
        merged = bbrun.merge_tu_costs(reps)["a.cpp"]
        self.assertEqual(merged["user_us"], 100)
        self.assertEqual(merged["instructions"], 105.0)
        self.assertEqual(merged["peak_rss_kb"], 30)
        self.assertEqual(merged["runs"], 3)

    def test_merge_tu_costs_tolerates_a_tu_missing_from_a_rep(self) -> None:
        reps = [
            {"a.cpp": bbparse.TuCost("a.cpp", compiles=1, user_us=100)},
            {},
        ]
        merged = bbrun.merge_tu_costs(reps)
        self.assertEqual(merged["a.cpp"]["runs"], 1)


class CompareTest(TempDirTest):
    def setUp(self) -> None:
        super().setUp()
        self._orig_dir = bbrun._buildbench_dir
        bbrun._buildbench_dir = lambda: self.tmp  # type: ignore

    def tearDown(self) -> None:
        bbrun._buildbench_dir = self._orig_dir  # type: ignore
        super().tearDown()

    def _write(self, name: str, instructions: float, spread: float = 0.0,
               per_tu: dict = None) -> None:
        payload = {
            "created": "now",
            "name": name,
            "ya_argv": ["./ya", "make", "ydb/core"],
            "repeat": 3,
            "tiers": {"perf": True},
            "points": [{
                "jobs": 64,
                "cpus": None,
                "runs": [],
                "metrics": {
                    "sum_tu_instructions": {
                        "median": instructions, "min": instructions,
                        "max": instructions, "spread_pct": spread, "n": 3,
                    },
                },
                "primary_metric": "sum_tu_instructions",
                "per_tu": per_tu or {},
            }],
        }
        (self.tmp / f"{name}.json").write_text(json.dumps(payload),
                                               encoding="utf-8")

    def _compare(self, before: str, after: str, top: int = 5) -> str:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = bbrun.compare_reports(before, after, top)
        self.assertEqual(rc, 0)
        return buf.getvalue()

    def test_large_drop_is_reported_as_an_improvement(self) -> None:
        self._write("before", 1000.0)
        self._write("after", 900.0)
        self.assertIn("IMPROVED", self._compare("before", "after"))

    def test_tiny_change_is_dismissed_as_noise(self) -> None:
        self._write("before", 1000.0)
        self._write("after", 996.0)
        out = self._compare("before", "after")
        self.assertIn("noise", out)
        self.assertNotIn("IMPROVED", out)

    def test_a_noisy_baseline_widens_the_band(self) -> None:
        # The same 10% drop is believable against a steady baseline and
        # meaningless against one that swung 9% on its own.
        self._write("before", 1000.0, spread=9.0)
        self._write("after", 900.0, spread=9.0)
        self.assertIn("noise", self._compare("before", "after"))

    def test_regression_is_named(self) -> None:
        self._write("before", 1000.0)
        self._write("after", 1200.0)
        self.assertIn("REGRESSED", self._compare("before", "after"))

    def test_per_tu_movements_are_listed(self) -> None:
        before = {"ydb/a.cpp": {"instructions": 100.0, "user_us": 10}}
        after = {"ydb/a.cpp": {"instructions": 50.0, "user_us": 5}}
        self._write("before", 1000.0, per_tu=before)
        self._write("after", 500.0, per_tu=after)
        out = self._compare("before", "after")
        self.assertIn("ydb/a.cpp", out)
        self.assertIn("-50.0%", out)


class TopTusTest(unittest.TestCase):
    def _capture(self, per_tu: dict, top: int = 5) -> str:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            bbrun.print_top_tus(per_tu, top)
        return buf.getvalue()

    def _record(self, **kwargs) -> dict:
        base = {"instructions": 1e9, "user_us": 1_000_000, "peak_rss_kb": 1024,
                "execute_us": 0, "frontend_us": 0, "backend_us": 0}
        base.update(kwargs)
        return base

    def test_frontend_column_is_hidden_without_timetrace(self) -> None:
        out = self._capture({"ydb/a.cpp": self._record()})
        self.assertNotIn("front%", out)
        self.assertIn("--timetrace", out)

    def test_frontend_column_appears_with_timetrace(self) -> None:
        out = self._capture({"ydb/a.cpp": self._record(execute_us=1000,
                                                       frontend_us=850)})
        self.assertIn("front%", out)
        self.assertIn("85", out)
        self.assertNotIn("--timetrace", out)

    def test_untraced_tu_shows_a_dash_among_traced_ones(self) -> None:
        out = self._capture({
            "ydb/a.cpp": self._record(execute_us=1000, frontend_us=850),
            "ydb/b.cpp": self._record(instructions=2e9),
        })
        self.assertIn("front%", out)
        self.assertIn("-", out)

    def test_nothing_printed_for_empty_input(self) -> None:
        self.assertEqual(self._capture({}), "")
        self.assertEqual(self._capture({"ydb/a.cpp": self._record()}, top=0), "")


class FormattingTest(unittest.TestCase):
    def test_human_count_scales(self) -> None:
        self.assertEqual(bbparse.human_count(1.5e12), "1.50 T")
        self.assertEqual(bbparse.human_count(2.5e9), "2.50 G")
        self.assertEqual(bbparse.human_count(42), "42")

    def test_format_metric_uses_the_metric_unit(self) -> None:
        self.assertEqual(bbrun.format_metric("sum_tu_instructions", 1e12),
                         "1.00 T")
        self.assertEqual(bbrun.format_metric("wall_s", 12.34), "12.3 s")
        self.assertEqual(bbrun.format_metric("max_rss_kb", 2048), "2 MiB")


class SummaryRenderTest(unittest.TestCase):
    def test_renders_without_perf_and_warns(self) -> None:
        point = bbrun.Point(jobs=64)
        point.runs.append(bbrun.RunSample(jobs=64, wall_s=10.0,
                                          sum_tu_user_s=100.0))
        metrics = bbrun.point_metrics(point)
        payload = {
            "created": "now",
            "ya_argv": ["./ya", "make", "ydb/core"],
            "repeat": 1,
            "tiers": {"procstat": True, "perf": False, "timetrace": False},
            "points": [{"jobs": 64, "cpus": None, "metrics": metrics}],
        }
        text = bbrun.render_summary("x", payload, [point])
        self.assertIn("# Build cost report: x", text)
        self.assertIn("`perf` counters were unavailable", text)
        self.assertIn("wall clock", text)


if __name__ == "__main__":
    unittest.main()
