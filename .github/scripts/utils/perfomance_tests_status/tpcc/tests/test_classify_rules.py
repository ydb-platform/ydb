#!/usr/bin/env python3
"""Unit tests for TPC-C compare-delta rules (mirror of template.html; no YDB / no HTML)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from classify_rules import (  # noqa: E402
    compare_delta_tpcc,
    include_row_in_compare,
    resolve_compare_row,
    tpcc_hard_band,
)
from generate import (  # noqa: E402
    allure_suite_for,
    attach_reports,
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


if __name__ == "__main__":
    unittest.main()
