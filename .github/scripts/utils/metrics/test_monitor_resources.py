#!/usr/bin/env python3
"""Unit tests for ya-tree CPU/I/O interval accounting."""

from __future__ import annotations

import unittest

from monitor_resources import cpu_delta_jiffies, io_delta_bytes, process_identity


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


if __name__ == "__main__":
    unittest.main()
