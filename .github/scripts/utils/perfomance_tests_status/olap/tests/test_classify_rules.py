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
    normalize_side_for_react,
    olap_hard_band,
    resolve_alert_cell,
    resolve_compare_cell,
    resolve_compare_row,
    short_cell_status,
)
from generate import (  # noqa: E402
    _query_metrics,
    collapse_in_progress_suite_dupes,
    promote_ok_with_hot_queries,
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


class CollapseInProgressDupesTests(unittest.TestCase):
    def test_drops_hot_and_ok_when_in_progress_covers_suite(self):
        suite = "Clickbench"
        hot = {
            "id": "main_db_suite",
            "issue": "failing",
            "branch": "main",
            "db": "sas_big_column",
            "suite": suite,
        }
        ip = {
            "id": "in_progress_main_db_suite",
            "issue": "in_progress",
            "branch": "main",
            "db": "sas_big_column",
            "suite": suite,
            "finished": {"issue": "failing", "status": "regression"},
        }
        ok = {
            "id": "ok_main_db_suite",
            "issue": "ok",
            "branch": "main",
            "db": "sas_big_column",
            "suite": suite,
        }
        by_id, oks = collapse_in_progress_suite_dupes({"h": hot, "ip": ip}, [ok])
        self.assertIn("ip", by_id)
        self.assertNotIn("h", by_id)
        self.assertEqual(oks, [])


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


class NormalizeSideForReactTests(unittest.TestCase):
    def test_fail_off_hides_failing_suite(self):
        react = {"fail": False, "hard": False, "soft": False, "nodata": False}
        got = normalize_side_for_react(_side("failing", n_fail=3), react)
        self.assertEqual(got["status"], "ok")
        self.assertEqual(got["n_fail"], 0)

    def test_fail_on_keeps_failing(self):
        got = normalize_side_for_react(_side("failing", n_fail=3), DEFAULT_REACT)
        self.assertEqual(got["status"], "failing")
        self.assertEqual(got["n_fail"], 3)

    def test_hard_off_hides_slow(self):
        got = normalize_side_for_react(_side("regression", n_slow=4), DEFAULT_REACT)
        self.assertEqual(got["status"], "ok")
        self.assertEqual(got["n_slow"], 0)

    def test_stale_with_nodata_becomes_nodata(self):
        react = {"fail": False, "hard": False, "soft": False, "nodata": True}
        got = normalize_side_for_react(_side("stale", n_nodata=18), react)
        self.assertEqual(got["status"], "nodata")
        self.assertEqual(got["n_nodata"], 18)

    def test_missing_stays_missing(self):
        react = {"fail": False, "hard": False, "soft": False, "nodata": True}
        got = normalize_side_for_react(_side("missing", n_nodata=2), react)
        self.assertEqual(got["status"], "missing")


class ResolveCompareCellTests(unittest.TestCase):
    """Published bug: alert ok (React fail off) → compare select → red 'fail ='."""

    def test_react_fail_off_equal_fails_is_ok_equals_not_fail(self):
        react = {"fail": False, "hard": False, "soft": False, "nodata": True}
        prev = _side("failing", n_fail=3)
        now = _side("failing", n_fail=3)
        cell = resolve_compare_cell(prev, now, react)
        self.assertEqual(cell["delta"], "same")
        self.assertEqual(cell["paint"], "ok")
        self.assertEqual(cell["label"], "ok =")
        self.assertFalse(cell["paint"].startswith("delta-"))
        self.assertNotIn("fail", cell["label"])

    def test_react_fail_on_equal_fails_is_fail_equals(self):
        prev = _side("failing", n_fail=3)
        now = _side("failing", n_fail=3)
        cell = resolve_compare_cell(prev, now, DEFAULT_REACT)
        self.assertEqual(cell["delta"], "same")
        self.assertEqual(cell["paint"], "failing")
        self.assertEqual(cell["label"], "fail 3 =")

    def test_uploadtpch_style_alert_ok_compare_same(self):
        # Alert path with fail off sees ok; compare must not invent red fail=.
        react = {"fail": False, "hard": False, "soft": False, "nodata": False}
        alert = resolve_alert_cell(_side("failing", n_fail=3, n_slow=0), react)
        self.assertEqual(alert["paint"], "ok")
        self.assertEqual(alert["label"], "ok")
        cell = resolve_compare_cell(
            _side("failing", n_fail=3), _side("failing", n_fail=3), react
        )
        self.assertEqual(cell["paint"], alert["paint"])
        self.assertEqual(cell["label"], "ok =")

    def test_real_fail_regression_still_delta_worse(self):
        prev = _side("ok")
        now = _side("failing", n_fail=2)
        cell = resolve_compare_cell(prev, now, DEFAULT_REACT)
        self.assertEqual(cell["delta"], "worse")
        self.assertEqual(cell["paint"], "delta-worse")
        self.assertEqual(cell["label"], "ok → fail 2")

    def test_paint_matches_label_contract(self):
        cases = [
            (_side("ok"), _side("failing", n_fail=3), {"fail": False, "hard": False, "soft": False, "nodata": False}),
            (_side("failing", n_fail=1), _side("failing", n_fail=1), DEFAULT_REACT),
            (_side("ok"), _side("ok", n_slow=5), DEFAULT_REACT),
            (_side("stale", n_nodata=47), _side("stale", n_nodata=47), {"fail": False, "hard": False, "soft": False, "nodata": True}),
        ]
        for prev, now, react in cases:
            with self.subTest(prev=prev, now=now, react=react):
                cell = resolve_compare_cell(prev, now, react)
                # Never paint delta-* when labels are equal.
                if " =" in cell["label"]:
                    self.assertEqual(cell["delta"], "same")
                    self.assertFalse(cell["paint"].startswith("delta-"))
                # Never show bare 'fail' when React.fail is off.
                if not react.get("fail"):
                    self.assertNotRegex(cell["label"], r"\bfail\b")


class ResolveAlertCellTests(unittest.TestCase):
    def test_nodata_counts_paint_nodata(self):
        react = {"fail": False, "hard": False, "soft": False, "nodata": True}
        cell = resolve_alert_cell(_side("ok", n_nodata=73), react)
        self.assertEqual(cell["paint"], "nodata")
        self.assertEqual(cell["label"], "no data 73")

    def test_empty_cell_is_noruns_not_nodata(self):
        react = {"fail": False, "hard": False, "soft": False, "nodata": True}
        cell = resolve_alert_cell(_side("nodata", n_nodata=0), react)
        self.assertEqual(cell["paint"], "noruns")
        self.assertEqual(cell["label"], "no runs")


class QueryNodataCompareTests(unittest.TestCase):
    """no data 73 → no data 73 must stay purple, never green ok =."""

    def test_equal_query_nodata_is_not_ok_equals(self):
        react = {"fail": False, "hard": False, "soft": False, "nodata": True}
        prev = _side("nodata", n_nodata=73)
        now = _side("nodata", n_nodata=73)
        cell = resolve_compare_cell(prev, now, react)
        self.assertEqual(cell["delta"], "same")
        self.assertEqual(cell["paint"], "nodata")
        self.assertEqual(cell["label"], "no data 73 =")
        self.assertNotIn("ok", cell["label"])

    def test_ok_to_nodata_is_delta_worse_purple(self):
        react = {"fail": True, "hard": False, "soft": False, "nodata": True}
        prev = _side("ok")
        now = _side("nodata", n_nodata=73)
        cell = resolve_compare_cell(prev, now, react)
        self.assertEqual(cell["delta"], "worse")
        self.assertTrue(cell["paint"].startswith("delta-worse"))
        self.assertIn("delta-nodata", cell["paint"])
        self.assertEqual(cell["label"], "ok → no data 73")

    def test_nodata_to_ok_is_delta_better_purple(self):
        react = {"fail": True, "hard": False, "soft": False, "nodata": True}
        prev = _side("nodata", n_nodata=18)
        now = _side("ok")
        cell = resolve_compare_cell(prev, now, react)
        self.assertEqual(cell["delta"], "better")
        self.assertIn("delta-nodata", cell["paint"])

    def test_nodata_count_up_is_worse(self):
        react = {"fail": False, "hard": False, "soft": False, "nodata": True}
        prev = _side("nodata", n_nodata=18)
        now = _side("nodata", n_nodata=47)
        self.assertEqual(compare_delta_olap(prev, now, react), "worse")

    def test_paint_class_splits_noruns_and_query_nodata(self):
        from classify_rules import paint_class_for_side

        self.assertEqual(paint_class_for_side(_side("noruns")), "noruns")
        self.assertEqual(paint_class_for_side(_side("nodata", n_nodata=0)), "noruns")
        self.assertEqual(paint_class_for_side(_side("nodata", n_nodata=18)), "nodata")


class QueryMetricsSinglePointTests(unittest.TestCase):
    """Solo history point must still mark fail (heatmap/inbox used to diverge)."""

    def test_single_point_fail_is_kind_fail(self):
        m = _query_metrics([{"fr": 1.0, "ydb": 100.0, "ts": "2026-07-26"}])
        self.assertIsNotNone(m)
        self.assertEqual(m["kind"], "fail")
        self.assertTrue(m["is_fail"])

    def test_single_point_ok_stays_ok(self):
        m = _query_metrics([{"fr": 0.0, "ydb": 100.0, "ts": "2026-07-26"}])
        self.assertIsNotNone(m)
        self.assertEqual(m["kind"], "ok")

    def test_promote_ok_suite_with_solo_fail_query(self):
        data = {
            "ok": [
                {
                    "branch": "stable-26-3",
                    "db": "sas_small_column",
                    "suite": "UploadTpch1",
                    "issue": "ok",
                    "status": "ok",
                    "queries": [
                        {"test": "Query14", "kind": "fail", "fail_rate_late": 1.0},
                    ],
                }
            ],
            "inbox": [],
        }
        moved = promote_ok_with_hot_queries(data)
        self.assertEqual(moved, 1)
        self.assertEqual(data["ok"], [])
        self.assertEqual(data["inbox"][0]["issue"], "failing")

    def test_promote_capped_by_inbox_returns_to_ok(self):
        """Inbox slower-cap must not erase a suite into fake «no runs»."""
        branch = "stable-26-3-1"
        db = "sas_small_column"
        # Saturate per-branch slower cap so the Clickbench promotion is dropped.
        inbox = [
            {
                "branch": branch,
                "db": db,
                "suite": f"TpchFlood{i}",
                "issue": "slower",
                "status": "regression",
                "ydb_pct": 0.9,
            }
            for i in range(30)
        ]
        data = {
            "ui": {"focus_branches": ["main", branch]},
            "ok": [
                {
                    "branch": branch,
                    "db": db,
                    "suite": "Clickbench",
                    "issue": "ok",
                    "status": "ok",
                    "queries": [
                        {"test": "Q01", "kind": "slow", "ydb_pct": 0.2},
                    ],
                }
            ],
            "inbox": inbox,
        }
        moved = promote_ok_with_hot_queries(data)
        self.assertEqual(moved, 1)
        inbox_suites = {r["suite"] for r in data["inbox"]}
        ok_suites = {r["suite"] for r in data["ok"]}
        self.assertNotIn("Clickbench", inbox_suites)
        self.assertIn("Clickbench", ok_suites)
        demoted = next(r for r in data["ok"] if r["suite"] == "Clickbench")
        self.assertEqual(demoted["issue"], "ok")
        self.assertTrue(demoted.get("query_promote_capped"))
        self.assertEqual(len(demoted.get("queries") or []), 1)


if __name__ == "__main__":
    unittest.main()
