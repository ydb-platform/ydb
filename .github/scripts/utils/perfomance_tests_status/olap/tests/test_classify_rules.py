#!/usr/bin/env python3
"""Unit tests for OLAP classify / compare rules (no YDB / no HTML)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from classify_rules import (  # noqa: E402
    FAIL_HOT,
    classify_duration,
    compare_delta_olap,
    fail_status_from_last,
    is_fail_rate_hot,
)


class FailRuleTests(unittest.TestCase):
    def test_hot_without_rise(self):
        # Chronically high fail (≥10%) stays hot even if baseline is also high.
        self.assertTrue(is_fail_rate_hot(0.12))
        st, reasons = fail_status_from_last(0.12, fr_base=0.11)
        self.assertEqual(st, "regression")  # Python status; JS maps this to failing (not broken)
        self.assertTrue(reasons)

    def test_below_hot(self):
        self.assertFalse(is_fail_rate_hot(FAIL_HOT - 0.001))
        st, reasons = fail_status_from_last(0.05, fr_base=0.0)
        self.assertEqual(st, "ok")
        self.assertEqual(reasons, [])

    def test_broken_threshold(self):
        st, _ = fail_status_from_last(0.55, fr_base=0.0)
        self.assertEqual(st, "broken")

    def test_mid_fail_not_broken(self):
        st, _ = fail_status_from_last(0.20, fr_base=0.0)
        self.assertEqual(st, "regression")
        self.assertNotEqual(st, "broken")


class DurationRuleTests(unittest.TestCase):
    def test_hard_slow(self):
        base = [100.0] * 7
        dur = classify_duration(30.0, 130.0, 100.0, [130.0], base)
        self.assertEqual(dur["status"], "regression")
        self.assertEqual(dur["level"], "hard")

    def test_outlier_broken(self):
        base = [100.0] * 7
        dur = classify_duration(400.0, 400.0, 100.0, [400.0], base)
        self.assertEqual(dur["status"], "broken")

    def test_ok_within_noise(self):
        base = [100.0, 140.0, 90.0, 120.0, 110.0, 95.0, 130.0]
        dur = classify_duration(12.0, 112.0, 100.0, [112.0], base)
        self.assertIn(dur["status"], ("ok", "watch"))


class CompareDeltaOlapTests(unittest.TestCase):
    def test_worse_fail_up(self):
        prev = {"status": "ok", "n_fail": 0, "n_slow": 0}
        now = {"status": "broken", "n_fail": 2, "n_slow": 0}
        self.assertEqual(compare_delta_olap(prev, now), "worse")

    def test_better_fail_down(self):
        prev = {"status": "broken", "n_fail": 3, "n_slow": 0}
        now = {"status": "ok", "n_fail": 0, "n_slow": 0}
        self.assertEqual(compare_delta_olap(prev, now), "better")

    def test_mixed_fail_up_slow_down(self):
        prev = {"status": "regression", "n_fail": 0, "n_slow": 4}
        now = {"status": "broken", "n_fail": 2, "n_slow": 1}
        self.assertEqual(compare_delta_olap(prev, now), "mixed")

    def test_same_soft_only(self):
        prev = {"status": "watch", "n_fail": 0, "n_slow": 0}
        now = {"status": "ok", "n_fail": 0, "n_slow": 0}
        self.assertEqual(compare_delta_olap(prev, now), "same")


if __name__ == "__main__":
    unittest.main()
