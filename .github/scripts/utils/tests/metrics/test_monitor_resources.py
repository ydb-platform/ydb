#!/usr/bin/env python3
"""Unit tests for ya-tree CPU/I/O interval accounting."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import mock_open, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _paths import METRICS, add_product_paths

add_product_paths(METRICS)

from monitor_resources import CPU_STATES, apply_io_sample, cpu_delta_jiffies, cpu_usage, io_delta_bytes, process_identity, read_proc_stat


class HostCpuAccountingTest(unittest.TestCase):
    def test_steal_is_not_cpu_execution(self):
        previous = dict.fromkeys(CPU_STATES, 100)
        increments = (20, 5, 10, 30, 10, 3, 2, 20)
        current = {state: previous[state] + delta for state, delta in zip(CPU_STATES, increments)}
        elapsed, usage = cpu_usage(previous, current)
        self.assertEqual(elapsed, 100)
        self.assertEqual(usage, {
            "cpu_total_pct": 40.0,
            "cpu_user_pct": 25.0,
            "cpu_system_pct": 15.0,
            "cpu_steal_pct": 20.0,
            "cpu_iowait_pct": 10.0,
        })

    def test_guest_time_is_not_counted_twice(self):
        with patch("builtins.open", mock_open(read_data="cpu 20 5 10 30 10 3 2 20 7 2\n")):
            counters = read_proc_stat()
        elapsed, usage = cpu_usage(dict.fromkeys(CPU_STATES, 0), counters)
        self.assertEqual(elapsed, 100)
        self.assertEqual(usage["cpu_user_pct"], 25.0)

    def test_zero_interval(self):
        counters = dict.fromkeys(CPU_STATES, 100)
        elapsed, usage = cpu_usage(counters, counters)
        self.assertEqual(elapsed, 0)
        self.assertTrue(all(value == 0 for value in usage.values()))

    def test_decreasing_iowait_does_not_inflate_usage(self):
        previous = dict.fromkeys(CPU_STATES, 100)
        current = dict(previous, user=110, idle=110, iowait=90)
        elapsed, usage = cpu_usage(previous, current)
        self.assertEqual(elapsed, 20)
        self.assertEqual(usage["cpu_total_pct"], 50.0)
        self.assertEqual(usage["cpu_iowait_pct"], 0.0)


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


if __name__ == "__main__":
    unittest.main()
