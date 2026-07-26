#!/usr/bin/env python3
"""Unit tests for OLAP classify / compare rules (no YDB / no HTML)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from classify_rules import (  # noqa: E402
    DEFAULT_REACT,
    FAIL_HOT,
    REACT_FAIL_HARD,
    classify_duration,
    compare_cell_paint_status,
    compare_delta_olap,
    fail_status_from_last,
    fmt_fs,
    include_row_in_compare,
    is_fail_rate_hot,
    olap_hard_band,
    resolve_compare_row,
    short_cell_status,
)


def _side(status="ok", n_fail=0, n_slow=0, n_soft=0, n_nodata=0):
    return {
        "status": status,
        "n_fail": n_fail,
        "n_slow": n_slow,
        "n_soft": n_soft,
        "n_nodata": n_nodata,
    }


def _labels(prev, now, react=None):
    a = short_cell_status(
        prev["status"],
        prev.get("n_fail", 0),
        prev.get("n_slow", 0),
        prev.get("n_soft", 0),
        prev.get("n_nodata", 0),
        react,
    )
    b = short_cell_status(
        now["status"],
        now.get("n_fail", 0),
        now.get("n_slow", 0),
        now.get("n_soft", 0),
        now.get("n_nodata", 0),
        react,
    )
    return a, b


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
        prev = _side("ok", n_fail=0)
        now = _side("broken", n_fail=2)
        self.assertEqual(compare_delta_olap(prev, now), "worse")

    def test_better_fail_down(self):
        prev = _side("broken", n_fail=3)
        now = _side("ok", n_fail=0)
        self.assertEqual(compare_delta_olap(prev, now), "better")

    def test_mixed_fail_up_slow_down_needs_hard(self):
        prev = _side("regression", n_slow=4)
        now = _side("broken", n_fail=2, n_slow=1)
        # With default hard=off, slows are ignored → only fail↑ → worse (not mixed).
        self.assertEqual(compare_delta_olap(prev, now), "worse")
        self.assertEqual(compare_delta_olap(prev, now, REACT_FAIL_HARD), "mixed")

    def test_same_soft_only(self):
        prev = _side("watch", n_soft=2)
        now = _side("ok")
        self.assertEqual(compare_delta_olap(prev, now), "same")
        self.assertEqual(compare_delta_olap(prev, now, REACT_FAIL_HARD), "same")

    def test_default_react_slow_up_is_same(self):
        """Regression: hard off → label 'ok=' must not paint delta-worse."""
        prev = _side("ok")
        now = _side("ok", n_slow=3)
        self.assertEqual(olap_hard_band(now), 0)
        self.assertEqual(compare_delta_olap(prev, now), "same")
        self.assertEqual(compare_delta_olap(prev, now, REACT_FAIL_HARD), "worse")

    def test_default_react_slow_status_ignored(self):
        prev = _side("ok")
        now = _side("regression", n_slow=2)
        self.assertEqual(compare_delta_olap(prev, now), "same")
        self.assertEqual(compare_delta_olap(prev, now, REACT_FAIL_HARD), "worse")

    def test_fail_off_ignores_fail_counts(self):
        react = {"fail": False, "hard": False, "soft": False, "nodata": False}
        prev = _side("ok")
        now = _side("broken", n_fail=4)
        self.assertEqual(compare_delta_olap(prev, now, react), "same")

    def test_equal_counts_same(self):
        prev = _side("failing", n_fail=1, n_slow=2)
        now = _side("failing", n_fail=1, n_slow=2)
        self.assertEqual(compare_delta_olap(prev, now, REACT_FAIL_HARD), "same")

    def test_better_slow_down_with_hard(self):
        prev = _side("regression", n_slow=5)
        now = _side("ok", n_slow=0)
        self.assertEqual(compare_delta_olap(prev, now, REACT_FAIL_HARD), "better")
        self.assertEqual(compare_delta_olap(prev, now), "same")


class LabelPaintConsistencyTests(unittest.TestCase):
    """The published bug: equal labels ('ok =') with delta-worse gradient."""

    def test_identical_labels_imply_same_paint_default_react(self):
        cases = [
            (_side("ok"), _side("ok", n_slow=4)),
            (_side("ok", n_slow=1), _side("ok", n_slow=9)),
            (_side("watch", n_soft=1), _side("ok")),
            (_side("ok"), _side("regression", n_slow=2)),
            (_side("in_progress"), _side("in_progress")),
        ]
        for prev, now in cases:
            with self.subTest(prev=prev, now=now):
                a, b = _labels(prev, now, DEFAULT_REACT)
                delta = compare_delta_olap(prev, now, DEFAULT_REACT)
                if a == b:
                    self.assertEqual(
                        delta,
                        "same",
                        f"label '{a} =' must not paint {delta}",
                    )
                    paint = compare_cell_paint_status(prev, now, DEFAULT_REACT)
                    self.assertFalse(paint.startswith("delta-"))

    def test_slow_only_labels_ok_equals(self):
        prev, now = _side("ok"), _side("ok", n_slow=3)
        a, b = _labels(prev, now)
        self.assertEqual(a, "ok")
        self.assertEqual(b, "ok")
        self.assertEqual(fmt_fs(0, 3, 0, 0), "")  # hard off → empty
        self.assertEqual(compare_delta_olap(prev, now), "same")

    def test_hard_on_slow_changes_both_label_and_paint(self):
        prev, now = _side("ok"), _side("ok", n_slow=3)
        a, b = _labels(prev, now, REACT_FAIL_HARD)
        self.assertEqual(a, "ok")
        self.assertEqual(b, "slow 3")
        self.assertEqual(compare_delta_olap(prev, now, REACT_FAIL_HARD), "worse")
        self.assertEqual(
            compare_cell_paint_status(prev, now, REACT_FAIL_HARD), "delta-worse"
        )

    def test_soft_react_label_diff_without_paint(self):
        react = {"fail": True, "hard": False, "soft": True, "nodata": False}
        prev = _side("watch", n_soft=2)
        now = _side("ok")
        a, b = _labels(prev, now, react)
        self.assertEqual(a, "soft 2")
        self.assertEqual(b, "ok")
        self.assertEqual(compare_delta_olap(prev, now, react), "same")


class WaveCompareRowFilterTests(unittest.TestCase):
    def test_finished_skips_in_progress_without_twin(self):
        self.assertFalse(include_row_in_compare("in_progress", "finished"))
        self.assertIsNone(
            resolve_compare_row({"issue": "in_progress", "suite": "Tpch"}, "finished")
        )
        self.assertTrue(include_row_in_compare("failing", "finished"))

    def test_finished_unwraps_twin(self):
        row = {
            "issue": "in_progress",
            "branch": "main",
            "db": "sas_big_column",
            "suite": "Tpch",
            "finished": {"issue": "failing", "n_fail": 2, "status": "failing"},
        }
        self.assertTrue(
            include_row_in_compare("in_progress", "finished", has_finished=True)
        )
        got = resolve_compare_row(row, "finished")
        self.assertIsNotNone(got)
        self.assertEqual(got["issue"], "failing")
        self.assertEqual(got["suite"], "Tpch")
        self.assertEqual(got["db"], "sas_big_column")

    def test_all_unwraps_twin_keeps_stub_without(self):
        twin = resolve_compare_row(
            {
                "issue": "in_progress",
                "suite": "Clickbench",
                "finished": {"issue": "ok", "status": "ok"},
            },
            "all",
        )
        self.assertEqual(twin["issue"], "ok")
        stub = resolve_compare_row({"issue": "in_progress", "suite": "Tpcds"}, "all")
        self.assertEqual(stub["issue"], "in_progress")


class CoverageLabelTests(unittest.TestCase):
    def test_in_progress_beats_fail_counts(self):
        # Regression: blue cell must not be labeled "fail 1".
        self.assertEqual(
            short_cell_status("in_progress", n_fail=1, n_slow=0),
            "in progress",
        )
        self.assertEqual(short_cell_status("missing", n_fail=3), "missing")
        self.assertEqual(short_cell_status("stale", n_slow=2), "stale")


if __name__ == "__main__":
    unittest.main()
