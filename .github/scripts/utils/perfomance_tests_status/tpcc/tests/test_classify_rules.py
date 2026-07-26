#!/usr/bin/env python3
"""Unit tests for TPC-C compare-delta rules (mirror of template.html; no YDB / no HTML)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from classify_rules import (  # noqa: E402
    baseline_usable,
    classify_pair_values,
    compare_delta_tpcc,
    include_row_in_compare,
    issue_filter_from_live,
    resolve_compare_cell,
    resolve_compare_row,
    side_metric_label,
    tpcc_hard_band,
)
from generate import (  # noqa: E402
    allure_suite_for,
    attach_reports,
    classify_slice,
    collapse_in_progress_suite_dupes,
    mart_cluster_to_ci,
)


def _side(
    status="ok",
    n_lat=0,
    n_tpmc=0,
    n_broken=0,
    lat_pct=None,
    tpmc_pct=None,
):
    return {
        "status": status,
        "n_lat": n_lat,
        "n_tpmc": n_tpmc,
        "n_broken": n_broken,
        "lat_pct": lat_pct,
        "tpmc_pct": tpmc_pct,
    }


class CompareDeltaTpccTests(unittest.TestCase):
    def test_mixed_lat_worse_tpmc_better(self):
        prev = _side("lat", n_lat=1, n_tpmc=1, lat_pct=12.0, tpmc_pct=-15.0)
        now = _side("both", n_lat=1, n_tpmc=1, lat_pct=25.0, tpmc_pct=-2.0)
        self.assertEqual(compare_delta_tpcc(prev, now), "mixed")

    def test_worse_hot_lat_step(self):
        prev = _side("lat", n_lat=1, lat_pct=12.0, tpmc_pct=0.0)
        now = _side("lat", n_lat=1, lat_pct=25.0, tpmc_pct=0.0)
        self.assertEqual(compare_delta_tpcc(prev, now), "worse-hot")

    def test_worse_from_green(self):
        prev = _side("ok", lat_pct=2.0, tpmc_pct=1.0)
        now = _side("lat", n_lat=1, lat_pct=15.0, tpmc_pct=1.0)
        self.assertEqual(tpcc_hard_band(prev), 0)
        self.assertEqual(tpcc_hard_band(now), 1)
        self.assertEqual(compare_delta_tpcc(prev, now), "worse")

    def test_better_recovery(self):
        prev = _side("lat", n_lat=1, lat_pct=18.0)
        now = _side("ok", lat_pct=2.0)
        self.assertEqual(compare_delta_tpcc(prev, now), "better")

    def test_same_sub_threshold_noise(self):
        # Both hard, but step < tol (10pp) → same (no paint flicker).
        prev = _side("lat", n_lat=1, lat_pct=12.0, tpmc_pct=0.0)
        now = _side("lat", n_lat=1, lat_pct=18.0, tpmc_pct=0.0)  # +6pp
        self.assertEqual(compare_delta_tpcc(prev, now), "same")

    def test_broken_up_worse_hot_when_already_hard(self):
        prev = _side("lat", n_lat=1, n_broken=0, lat_pct=12.0)
        now = _side("broken", n_lat=1, n_broken=1, lat_pct=12.0)
        self.assertEqual(compare_delta_tpcc(prev, now), "worse-hot")

    def test_broken_up_from_green_is_worse(self):
        prev = _side("ok")
        now = _side("broken", n_broken=1, lat_pct=40.0)
        self.assertEqual(compare_delta_tpcc(prev, now), "worse")

    def test_watch_only_same(self):
        prev = _side("ok", lat_pct=3.0)
        now = _side("watch", lat_pct=8.0)  # below hard band (status watch → band 0)
        self.assertEqual(tpcc_hard_band(now), 0)
        self.assertEqual(compare_delta_tpcc(prev, now), "same")

    def test_tpmc_worse_hot(self):
        prev = _side("tpmc", n_tpmc=1, lat_pct=0.0, tpmc_pct=-12.0)
        now = _side("tpmc", n_tpmc=1, lat_pct=0.0, tpmc_pct=-25.0)
        self.assertEqual(compare_delta_tpcc(prev, now), "worse-hot")

    def test_missing_pct_no_step_crash(self):
        prev = _side("ok")
        now = _side("ok", lat_pct=5.0)
        self.assertEqual(compare_delta_tpcc(prev, now), "same")


class CollapseInProgressDupesTests(unittest.TestCase):
    def test_drops_hot_and_ok_when_in_progress_covers_suite(self):
        suite = "serializable_default@20000"
        hot = {
            "id": "main_perf4_suite",
            "issue": "lat",
            "branch": "main",
            "db": "perf4",
            "suite": suite,
            "wh_label": "20k",
        }
        ip = {
            "id": "in_progress_main_perf4_suite",
            "issue": "in_progress",
            "branch": "main",
            "db": "perf4",
            "suite": suite,
            "wh_label": "20k",
            "finished": {"issue": "lat", "status": "regression"},
        }
        ok = {
            "id": "ok_main_perf4_suite",
            "issue": "ok",
            "branch": "main",
            "db": "perf4",
            "suite": suite,
        }
        other = {
            "id": "main_perf3_other",
            "issue": "lat",
            "branch": "main",
            "db": "perf3",
            "suite": "snapshot_default@20000",
        }
        by_id, oks = collapse_in_progress_suite_dupes(
            {"h": hot, "ip": ip, "o": other}, [ok]
        )
        self.assertIn("ip", by_id)
        self.assertIn("o", by_id)
        self.assertNotIn("h", by_id)
        self.assertEqual(oks, [])


class WaveCompareRowFilterTests(unittest.TestCase):
    def test_finished_skips_in_progress_without_twin(self):
        self.assertFalse(include_row_in_compare("in_progress", "finished"))
        self.assertIsNone(
            resolve_compare_row({"issue": "in_progress", "suite": "snapshot"}, "finished")
        )

    def test_finished_unwraps_twin(self):
        row = {
            "issue": "in_progress",
            "db": "perf3",
            "suite": "snapshot_default@20000",
            "finished": {"issue": "lat", "status": "lat", "lat_pct": 18.0},
        }
        got = resolve_compare_row(row, "finished")
        self.assertEqual(got["issue"], "lat")
        self.assertEqual(got["suite"], "snapshot_default@20000")

    def test_all_unwraps_twin(self):
        got = resolve_compare_row(
            {
                "issue": "in_progress",
                "suite": "x",
                "finished": {"issue": "ok", "status": "ok"},
            },
            "all",
        )
        self.assertEqual(got["issue"], "ok")


class ClassifySliceDriftTests(unittest.TestCase):
    def _pts(self, tpmcs, lat=800.0):
        from datetime import datetime, timedelta, timezone

        t0 = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
        out = []
        for i, tpmc in enumerate(tpmcs):
            ts = t0 + timedelta(days=i)
            out.append(
                {
                    "ts": ts,
                    "ts_iso": ts.isoformat().replace("+00:00", "Z"),
                    "tpmc": float(tpmc),
                    "lat90": float(lat),
                    "lat_raw": float(lat),
                    "lat_capped": False,
                    "version": f"c{i:04d}",
                    "label": f"{ts.date().isoformat()}_c{i:04d}",
                    "report": None,
                }
            )
        return out

    def test_gradual_tpmc_drift_is_watch_not_hot(self):
        # Early ~250k (p90 anchor), then prev7 already walked to ~242k, now ~241k.
        lookback = [250000 + (i % 5) * 200 for i in range(21)]
        prev7 = [242000 + i * 50 for i in range(7)]
        now = [240994]
        info = classify_slice(self._pts(lookback + prev7 + now))
        self.assertEqual(info["status"], "watch")
        self.assertEqual(info["kind"], "tpmc")
        self.assertIsNotNone(info["tpmc_drift_pct"])
        self.assertLessEqual(info["tpmc_drift_pct"], -3.5)
        self.assertGreater(info["tpmc_pct"], -10)  # not alert-hot vs prev7
        self.assertTrue(any("drift" in r for r in info["reasons"]))
        self.assertTrue(any("p90" in r for r in info["reasons"]))

    def test_sharp_tpmc_drop_still_hot(self):
        base = [250000] * 28
        info = classify_slice(self._pts(base + [220000]))  # −12% vs prev7
        self.assertEqual(info["status"], "regression")
        self.assertEqual(info["kind"], "tpmc")
        self.assertLessEqual(info["tpmc_pct"], -10)


class ReportJoinTests(unittest.TestCase):
    def test_cluster_and_suite_mapping(self):
        self.assertEqual(mart_cluster_to_ci("perf3"), "oltp-perf-3")
        self.assertEqual(
            allure_suite_for("ydb_cli_snapshot_default", 20000),
            "TpccW20000T0Snapshot",
        )
        self.assertEqual(
            allure_suite_for("ydb_cli_serializable_latency", 12000),
            "TpccW12000T0Serializable",
        )

    def test_attach_nearest_report(self):
        from datetime import datetime, timezone

        ts = datetime(2026, 7, 26, 15, 20, tzinfo=timezone.utc)
        points = [
            {
                "cluster": "perf3",
                "run_type": "ydb_cli_snapshot_default",
                "warehouses": 20000,
                "ts": ts,
                "report": None,
            }
        ]
        reports = [
            {
                "Suite": "TpccW20000T0Snapshot",
                "ci_cluster_name": "oltp-perf-3",
                "report_url": "https://proxy.sandbox.yandex-team.ru/111/index.html",
                "timestamp": "2026-07-26T15:21:57Z",
            },
            {
                "Suite": "TpccW20000T0Snapshot",
                "ci_cluster_name": "oltp-perf-3",
                "report_url": "https://proxy.sandbox.yandex-team.ru/222/index.html",
                "timestamp": "2026-07-26T07:33:06Z",
            },
        ]
        n = attach_reports(points, reports)
        self.assertEqual(n, 1)
        self.assertIn("/111/", points[0]["report"])


class ClassifyPairBaselineTests(unittest.TestCase):
    """Window-edge compare: empty prev7 must not classify as ok."""

    def test_null_baseline_returns_none(self):
        self.assertFalse(baseline_usable(None, None))
        self.assertIsNone(classify_pair_values(None, None, 358.0, 148194.0, False))

    def test_null_baseline_capped_still_broken(self):
        got = classify_pair_values(None, None, None, 148194.0, True)
        self.assertIsNotNone(got)
        self.assertEqual(got["status"], "broken")

    def test_outlier_with_baseline(self):
        got = classify_pair_values(64.0, 151000.0, 358.0, 148194.0, False)
        self.assertEqual(got["status"], "broken")
        self.assertEqual(got["n_broken"], 1)

    def test_ok_with_baseline(self):
        got = classify_pair_values(64.0, 151000.0, 66.0, 151100.0, False)
        self.assertEqual(got["status"], "ok")


class ResolveCompareCellTpccTests(unittest.TestCase):
    def test_equal_ok_label(self):
        prev = _side("ok", lat_pct=1.0, tpmc_pct=0.0)
        now = _side("ok", lat_pct=1.0, tpmc_pct=0.0)
        cell = resolve_compare_cell(prev, now)
        self.assertEqual(cell["delta"], "same")
        self.assertEqual(cell["paint"], "ok")
        self.assertTrue(cell["label"].endswith("=") or " =" in cell["label"])

    def test_broken_same_not_ok_equals(self):
        prev = _side("broken", n_broken=1, n_lat=1, lat_pct=400.0)
        now = _side("broken", n_broken=1, n_lat=1, lat_pct=459.0)
        cell = resolve_compare_cell(prev, now)
        self.assertNotEqual(cell["paint"], "ok")
        self.assertFalse(cell["label"].startswith("ok"))
        self.assertNotEqual(cell["label"], "ok =")

    def test_issue_filter_from_live_ok_and_broken(self):
        self.assertEqual(
            issue_filter_from_live({"compare": True, "delta_status": "ok", "status": "ok"}),
            "ok",
        )
        self.assertEqual(
            issue_filter_from_live({"compare": False, "status": "broken"}),
            "broken",
        )
        self.assertEqual(side_metric_label(_side("ok")), "ok")


if __name__ == "__main__":
    unittest.main()
