#!/usr/bin/env python3
"""Unit tests for ya-tree CPU/I/O interval accounting."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _paths import METRICS, add_product_paths

add_product_paths(METRICS)

from monitor_resources import (
    TestCgroupSampler,
    _parse_diskstats,
    _parse_pressure,
    _parse_proc_stat,
    apply_io_sample,
    chunk_from_text,
    cpu_delta_jiffies,
    discover_test_cgroups,
    identity_quality,
    io_delta_bytes,
    is_generic_suite,
    parse_cmdline_flag,
    process_identity,
    pressure_interval_pct,
    read_cpu_usage_usec,
    read_io_bytes,
    read_memory_current_bytes,
    read_memory_events,
    read_psi_total_usec,
    read_cpu_stat,
    suite_from_exe,
    suite_from_workdir,
)


class HostResourceParserTest(unittest.TestCase):
    def test_proc_stat_keeps_iowait_separate_from_busy_cpu(self):
        total, idle_including_iowait, iowait = _parse_proc_stat(
            "cpu 10 1 2 80 5 1 1 1 0 0\n"
        )
        self.assertEqual((total, idle_including_iowait, iowait), (101.0, 85.0, 5.0))

    def test_pressure_parses_some_and_full_totals(self):
        previous = {"cpu": {"some": 1000}, "memory": {"some": 2000, "full": 500}}
        current = {"cpu": {"some": 51000}, "memory": {"some": 22000, "full": 10500}}
        text = "some avg10=0.20 avg60=0.10 avg300=0.02 total=51000\n"
        parsed = _parse_pressure(text)
        self.assertEqual(parsed, {"some": 51000})
        self.assertEqual(
            pressure_interval_pct(
                {"cpu": parsed, "memory": current["memory"]},
                previous,
                1.0,
            ),
            {
                "pressure_cpu_some_pct": 5.0,
                "pressure_memory_some_pct": 2.0,
                "pressure_memory_full_pct": 1.0,
            },
        )

    def test_diskstats_reports_root_device_queue_and_busy_counters(self):
        diskstats = """\
8 0 sda 100 0 200 3 50 0 400 5 2 600 800
8 1 sda1 20 0 40 1 10 0 80 2 0 100 120
253 0 dm-0 30 0 60 1 15 0 120 2 1 200 250
"""
        self.assertEqual(_parse_diskstats(diskstats, {"sda", "dm-0"}), (200, 400, 2, 600, 800, 1))


class ProcessAccountingTest(unittest.TestCase):
    def test_identity_uses_starttime(self):
        self.assertEqual(process_identity({"pid": 10, "starttime": 99}), (10, 99))

    def test_new_process_contributes_lifetime_cpu(self):
        self.assertEqual(cpu_delta_jiffies(None, 30, 20), 50)

    def test_known_process_uses_interval_delta(self):
        self.assertEqual(cpu_delta_jiffies((10, 5), 40, 15), 40)

    def test_negative_cpu_delta_clamped(self):
        self.assertEqual(cpu_delta_jiffies((100, 0), 10, 0), 0)

    def test_new_process_io_uses_lifetime_counters(self):
        self.assertEqual(io_delta_bytes(None, (8, 2)), (8, 2))

    def test_known_process_io_is_per_pid(self):
        self.assertEqual(io_delta_bytes((100, 50), (140, 55)), (40, 5))

    def test_vanished_pid_does_not_make_negative_io(self):
        self.assertEqual(io_delta_bytes((200, 80), (10, 5)), (0, 0))

    def test_failed_io_read_does_not_baseline_at_zero(self):
        ident = (1, 10)
        prev: dict = {}
        nxt: dict = {}
        self.assertEqual(apply_io_sample(ident, None, prev, nxt), (0, 0))
        self.assertIsNone(nxt[ident])
        nxt2: dict = {}
        self.assertEqual(apply_io_sample(ident, (500, 20), nxt, nxt2), (0, 0))
        self.assertEqual(nxt2[ident], (500, 20))
        nxt3: dict = {}
        self.assertEqual(apply_io_sample(ident, (540, 25), nxt2, nxt3), (40, 5))

    def test_first_good_io_read_still_counts_lifetime(self):
        nxt: dict = {}
        self.assertEqual(apply_io_sample((2, 1), (8, 2), {}, nxt), (8, 2))
        self.assertEqual(nxt[(2, 1)], (8, 2))


class TestCgroupSampleTest(unittest.TestCase):
    def test_parse_project_path_and_chunk(self):
        cmd = "test_tool --project-path ydb/core/kqp --split-index 3"
        self.assertEqual(parse_cmdline_flag(cmd, "--project-path"), "ydb/core/kqp")
        self.assertEqual(parse_cmdline_flag(cmd, "--split-index"), "3")
        self.assertEqual(parse_cmdline_flag("x --project-path=foo/bar", "--project-path"), "foo/bar")

    def test_reads_cgroup_stat_files(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        (root / "cpu.stat").write_text("usage_usec 2500000\nuser_usec 1\n")
        (root / "memory.current").write_text("2097152\n")
        (root / "io.stat").write_text("8:0 rbytes=1048576 wbytes=2097152 rios=1 wios=1\n")
        (root / "memory.events").write_text("low 0\nhigh 3\nmax 1\noom 0\noom_kill 0\n")
        (root / "cpu.pressure").write_text("some avg10=0.00 avg60=0.00 avg300=0.00 total=1234\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=234\n")
        self.assertEqual(read_cpu_usage_usec(root), 2500000)
        self.assertEqual(read_cpu_stat(root)["usage_usec"], 2500000)
        self.assertEqual(read_memory_current_bytes(root), 2097152)
        self.assertEqual(read_io_bytes(root), (1048576, 2097152))
        self.assertEqual(read_memory_events(root)["high"], 3)
        self.assertEqual(read_psi_total_usec(root, "cpu"), {"some": 1234, "full": 234})

    def test_discovers_ya_test_dirs(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        (root / "ya-runtime").mkdir()
        (root / "ya-test-1-2").mkdir()
        self.assertEqual([p.name for p in discover_test_cgroups(root)], ["ya-test-1-2"])

    def test_first_tick_is_baseline_second_emits_cores(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        cgroup = root / "ya-test-9-1"
        cgroup.mkdir()
        (cgroup / "cpu.stat").write_text("usage_usec 1000000\nnr_periods 40\nnr_throttled 0\nthrottled_usec 0\n")
        (cgroup / "memory.current").write_text("1048576\n")
        (cgroup / "io.stat").write_text("8:0 rbytes=0 wbytes=0\n")
        (cgroup / "memory.events").write_text("low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n")
        (cgroup / "cpu.pressure").write_text("some avg10=0 avg60=0 avg300=0 total=0\nfull avg10=0 avg60=0 avg300=0 total=0\n")
        (cgroup / "memory.pressure").write_text("some avg10=0 avg60=0 avg300=0 total=0\nfull avg10=0 avg60=0 avg300=0 total=0\n")
        (cgroup / "io.pressure").write_text("some avg10=0 avg60=0 avg300=0 total=0\nfull avg10=0 avg60=0 avg300=0 total=0\n")
        (cgroup / "cgroup.procs").write_text("")
        sampler = TestCgroupSampler()
        self.assertEqual(sampler.sample(1.0, 10.0, 10_000_000, {}, extra_root=root), [])
        (cgroup / "cpu.stat").write_text("usage_usec 3000000\nnr_periods 40\nnr_throttled 5\nthrottled_usec 400000\n")
        (cgroup / "io.stat").write_text("8:0 rbytes=1048576 wbytes=0\n")
        (cgroup / "memory.events").write_text("low 0\nhigh 2\nmax 1\noom 1\noom_kill 0\n")
        (cgroup / "cpu.pressure").write_text("some avg10=0 avg60=0 avg300=0 total=100000\nfull avg10=0 avg60=0 avg300=0 total=50000\n")
        (cgroup / "memory.pressure").write_text("some avg10=0 avg60=0 avg300=0 total=200000\nfull avg10=0 avg60=0 avg300=0 total=100000\n")
        (cgroup / "io.pressure").write_text("some avg10=0 avg60=0 avg300=0 total=300000\nfull avg10=0 avg60=0 avg300=0 total=150000\n")
        rows = sampler.sample(1.0, 11.0, 11_000_000, {}, extra_root=root)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["kind"], "test")
        self.assertEqual(rows[0]["cgroup"], "ya-test-9-1")
        self.assertAlmostEqual(rows[0]["cpu_used_cores"], 2.0)
        self.assertEqual(rows[0]["ram_kb"], 1024)
        self.assertAlmostEqual(rows[0]["disk_read_mbps"], 1.0)
        self.assertEqual(rows[0]["cpu_nr_throttled_delta"], 5)
        self.assertEqual(rows[0]["cpu_throttled_usec_delta"], 400000)
        self.assertAlmostEqual(rows[0]["cpu_throttled_pct"], 40.0)
        self.assertAlmostEqual(rows[0]["cpu_psi_some_pct"], 10.0)
        self.assertAlmostEqual(rows[0]["cpu_psi_full_pct"], 5.0)
        self.assertAlmostEqual(rows[0]["memory_psi_some_pct"], 20.0)
        self.assertAlmostEqual(rows[0]["io_psi_some_pct"], 30.0)
        self.assertEqual(rows[0]["memory_high_events_delta"], 2)
        self.assertEqual(rows[0]["memory_max_events_delta"], 1)
        self.assertEqual(rows[0]["memory_oom_events_delta"], 1)
        self.assertEqual(rows[0]["memory_oom_kill_events_delta"], 0)


class TestIdentityFromPaths(unittest.TestCase):
    def test_workdir_uses_test_results_parent(self):
        path = (
            "/home/runner/work/ydb/ydb/ydb/core/tx/schemeshard/ut_olap_reboots/"
            "test-results/unittest/testing_out_stuff/chunk5"
        )
        self.assertEqual(suite_from_workdir(path), "ydb/core/tx/schemeshard/ut_olap_reboots")
        self.assertEqual(chunk_from_text(path), 5)

    def test_exe_drops_binary_name(self):
        path = "/home/runner/work/ydb/ydb/ydb/core/tx/schemeshard/ut_olap_reboots/ut_olap_reboots"
        self.assertEqual(suite_from_exe(path), "ydb/core/tx/schemeshard/ut_olap_reboots")

    def test_generic_unittest_is_not_a_suite(self):
        self.assertTrue(is_generic_suite("unittest"))
        self.assertTrue(is_generic_suite("ya-test-802992-609"))
        self.assertFalse(is_generic_suite("ydb/core/tx/schemeshard/ut_olap_reboots"))
        self.assertEqual(identity_quality({"suite": "unittest", "chunk": None}), 0)
        self.assertEqual(
            identity_quality({"suite": "ydb/core/tx/schemeshard/ut_olap_reboots", "chunk": 5}),
            3,
        )


if __name__ == "__main__":
    unittest.main()
