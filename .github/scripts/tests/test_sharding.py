#!/usr/bin/env python3
"""Unit tests for PR-check sharding helpers."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "sharding"
SHARDING_DIR = Path(__file__).resolve().parent / "sharding"

if str(SHARDING_DIR) not in sys.path:
    sys.path.insert(0, str(SHARDING_DIR))

from choose_shard_count import (  # noqa: E402
    choose_shard_count,
    enrich_plan_timing_estimate,
    estimate_critical_path_minutes,
)
from estimate_runner_capacity import compute_max_new_runners  # noqa: E402
from filter_graph_for_shard import filter_for_shard  # noqa: E402
from graph_plan_utils import assign_result_uids_to_shards, load_graph  # noqa: E402
from render_artifacts_nav import render_nav_html  # noqa: E402
from render_shard_plan_summary import render as render_shard_plan_summary  # noqa: E402


def _run(script: str, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SHARDING_DIR / script), *args],
        check=True,
        capture_output=True,
        text=True,
    )


class ShardingToolsTest(unittest.TestCase):
    def test_extract_tests_filters_by_target_prefix(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "tests.txt"
            _run(
                "extract_tests_from_graph.py",
                str(FIXTURES / "graph_sample.json"),
                "--target-prefix",
                "ydb/tests/olap",
                "-o",
                str(out),
            )
            tests = out.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(
                tests,
                [
                    "ydb/tests/olap/load",
                    "ydb/tests/olap/s3_import",
                ],
            )

    def test_merge_build_reports(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "merged.json"
            _run(
                "merge_build_reports.py",
                "-o",
                str(out),
                str(FIXTURES / "report_shard_0.json"),
                str(FIXTURES / "report_shard_1.json"),
            )
            merged = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(len(merged["results"]), 4)

    def test_merge_deduplicates_overlapping_shard_runs_worst_wins(self):
        with tempfile.TemporaryDirectory() as tmp:
            shard_a = Path(tmp) / "a.json"
            shard_b = Path(tmp) / "b.json"
            row = {
                "path": "ydb/tests/olap/load",
                "name": "Py",
                "subtest_name": "case",
                "type": "test",
            }
            shard_a.write_text(
                json.dumps({"results": [{**row, "status": "OK"}]}),
                encoding="utf-8",
            )
            shard_b.write_text(
                json.dumps({"results": [{**row, "status": "FAILED"}]}),
                encoding="utf-8",
            )
            out = Path(tmp) / "merged.json"
            _run("merge_build_reports.py", "-o", str(out), str(shard_a), str(shard_b))
            merged = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(len(merged["results"]), 1)
            self.assertEqual(merged["results"][0]["status"], "FAILED")

    def test_split_test_shards_balances_by_duration(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "plan.json"
            _run(
                "split_test_shards.py",
                "--tests-file",
                str(FIXTURES / "tests.txt"),
                "--durations-file",
                str(FIXTURES / "durations.csv"),
                "--shard-count",
                "2",
                "-o",
                str(plan_path),
            )
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            self.assertEqual(plan["shard_count"], 2)
            shard_tests = [set(shard["tests"]) for shard in plan["shards"]]
            self.assertEqual(set.union(*shard_tests), set(FIXTURES.joinpath("tests.txt").read_text().strip().split()))
            loads = [shard["estimated_duration_sec"] for shard in plan["shards"]]
            self.assertLess(max(loads) - min(loads), 60)

    def test_split_caps_shard_count_to_suite_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "plan.json"
            _run(
                "split_test_shards.py",
                "--tests-file",
                str(FIXTURES / "tests.txt"),
                "--shard-count",
                "4",
                "-o",
                str(plan_path),
            )
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            self.assertEqual(plan["requested_shard_count"], 4)
            self.assertEqual(plan["shard_count"], 2)
            self.assertEqual(len(plan["shards"]), 2)
            self.assertTrue(all(shard["tests"] for shard in plan["shards"]))

    def test_split_keeps_suite_directories_atomic(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "plan.json"
            _run(
                "split_test_shards.py",
                "--tests-file",
                str(FIXTURES / "tests.txt"),
                "--durations-file",
                str(FIXTURES / "durations.csv"),
                "--shard-count",
                "2",
                "-o",
                str(plan_path),
            )
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            suites = FIXTURES.joinpath("tests.txt").read_text().strip().split()
            for suite in suites:
                shard_ids = {
                    shard["id"]
                    for shard in plan["shards"]
                    if suite in shard["tests"]
                }
                self.assertEqual(len(shard_ids), 1, f"suite {suite} split across shards")

    def test_extract_drops_scope_root_when_nested_suites_exist(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "tests.txt"
            _run(
                "extract_tests_from_graph.py",
                str(FIXTURES / "graph_sample.json"),
                "--target-prefix",
                "ydb/tests/olap",
                "-o",
                str(out),
            )
            tests = out.read_text(encoding="utf-8").strip().splitlines()
            self.assertNotIn("ydb/tests/olap", tests)
            self.assertEqual(
                tests,
                ["ydb/tests/olap/load", "ydb/tests/olap/s3_import"],
            )

    def test_extract_failed_test_filters(self):
        proc = subprocess.run(
            [sys.executable, str(SHARDING_DIR / "extract_failed_test_filters.py"), str(FIXTURES / "report_shard_0.json")],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.stdout.strip().splitlines(), ["Suite::Slow"])

    def test_extract_failed_suite_paths(self):
        proc = subprocess.run(
            [sys.executable, str(SHARDING_DIR / "extract_failed_suite_paths.py"), str(FIXTURES / "report_shard_0.json")],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.stdout.strip().splitlines(), ["ydb/core/foo/ut"])

    def test_extract_failed_suite_paths_includes_chunk_failures(self):
        with tempfile.TemporaryDirectory() as tmp:
            report_path = Path(tmp) / "report.json"
            report_path.write_text(
                json.dumps(
                    {
                        "results": [
                            {
                                "path": "ydb/core/kqp/ut/scheme",
                                "name": "KqpConstraints",
                                "subtest_name": "AlterTableSetDropDefaultAsyncIndexOnColumn",
                                "status": "FAILED",
                                "type": "test",
                            },
                            {
                                "path": "ydb/core/tx/datashard/ut_read_iterator",
                                "name": "ReadIteratorExternalBlobs",
                                "subtest_name": "chunk 3",
                                "status": "FAILED",
                                "type": "test",
                                "chunk": True,
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )
            suites = subprocess.run(
                [sys.executable, str(SHARDING_DIR / "extract_failed_suite_paths.py"), str(report_path)],
                check=True,
                capture_output=True,
                text=True,
            )
            filters = subprocess.run(
                [sys.executable, str(SHARDING_DIR / "extract_failed_test_filters.py"), str(report_path)],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual(
                suites.stdout.strip().splitlines(),
                ["ydb/core/kqp/ut/scheme", "ydb/core/tx/datashard/ut_read_iterator"],
            )
            self.assertEqual(
                filters.stdout.strip().splitlines(),
                ["KqpConstraints::AlterTableSetDropDefaultAsyncIndexOnColumn"],
            )

    def test_extract_suites_from_ya_test_list_olap(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "tests.txt"
            summary = Path(tmp) / "summary.json"
            _run(
                "extract_suites_from_ya_test_list.py",
                str(FIXTURES / "ya_test_list_olap.log"),
                "--target-prefix",
                "ydb/tests/olap",
                "-o",
                str(out),
                "--summary-json",
                str(summary),
            )
            suites = out.read_text(encoding="utf-8").strip().splitlines()
            summary_data = json.loads(summary.read_text(encoding="utf-8"))
            self.assertIn("ydb/tests/olap", suites)
            self.assertIn("ydb/tests/olap/ttl_tiering", suites)
            self.assertEqual(summary_data["total_suites"], len(suites))
            self.assertGreater(summary_data["total_tests"], 0)
            self.assertEqual(
                summary_data["total_tests"],
                sum(item["test_count"] for item in summary_data["suites"]),
            )
            self.assertEqual(
                summary_data["total_weight"],
                sum(item["weight"] for item in summary_data["suites"]),
            )
            self.assertEqual(summary_data["size_weights"], {"small": 60, "medium": 600})

    def test_filter_suites_by_increment_graph(self):
        with tempfile.TemporaryDirectory() as tmp:
            full_summary = Path(tmp) / "full.json"
            _run(
                "extract_suites_from_ya_test_list.py",
                str(FIXTURES / "ya_test_list_olap.log"),
                "--target-prefix",
                "ydb/tests/olap",
                "--summary-json",
                str(full_summary),
            )
            filtered_summary = Path(tmp) / "filtered.json"
            filtered_tests = Path(tmp) / "filtered.txt"
            _run(
                "filter_suites_by_increment_graph.py",
                str(full_summary),
                str(FIXTURES / "graph_sample.json"),
                "--target-prefix",
                "ydb/tests/olap",
                "-o",
                str(filtered_tests),
                "--summary-json",
                str(filtered_summary),
            )
            data = json.loads(filtered_summary.read_text(encoding="utf-8"))
            self.assertTrue(data["increment_filtered"])
            self.assertEqual(data["increment_graph_suites"], 2)
            self.assertEqual(
                [suite["path"] for suite in data["suites"]],
                [],
            )
            self.assertEqual(data["total_suites"], 0)

    def test_extract_accepts_target_prefix_with_trailing_slash(self):
        with tempfile.TemporaryDirectory() as tmp:
            list_log = Path(tmp) / "list.log"
            list_log.write_text(
                "\n".join(
                    [
                        "ydb/core/foo/ut <unittest> [size:small] for default-linux-x86_64-debug",
                        "  Foo::Bar",
                        "",
                        "Total 1 suites",
                        "Total 1 tests",
                        "Ok",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            summary_path = Path(tmp) / "summary.json"
            _run(
                "extract_suites_from_ya_test_list.py",
                str(list_log),
                "--target-prefix",
                "ydb/",
                "--summary-json",
                str(summary_path),
            )
            data = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(data["total_suites"], 1)
            self.assertEqual(data["suites"][0]["path"], "ydb/core/foo/ut")

    def test_extract_treats_missing_size_as_small(self):
        with tempfile.TemporaryDirectory() as tmp:
            list_log = Path(tmp) / "list.log"
            list_log.write_text(
                "\n".join(
                    [
                        "ydb/core/log_backend/ut <unittest> for default-linux-x86_64-debug",
                        "  foo.cpp:Bar::Baz",
                        "  foo.cpp:Bar::Qux",
                        "",
                        "Total 1 suites",
                        "Total 2 tests",
                        "Ok",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            summary_path = Path(tmp) / "summary.json"
            _run(
                "extract_suites_from_ya_test_list.py",
                str(list_log),
                "--target-prefix",
                "ydb/core",
                "--summary-json",
                str(summary_path),
            )
            data = json.loads(summary_path.read_text(encoding="utf-8"))
            suite = data["suites"][0]
            self.assertEqual(suite["path"], "ydb/core/log_backend/ut")
            self.assertEqual(suite["sizes"], ["small"])
            self.assertEqual(suite["small_test_count"], 2)
            self.assertEqual(suite["medium_test_count"], 0)
            self.assertEqual(suite["weight"], 120)

    def test_filter_keeps_intersecting_suites(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary = {
                "size_weights": {"small": 60, "medium": 600},
                "suites": [
                    {
                        "path": "ydb/tests/olap/load",
                        "test_count": 5,
                        "weight": 3000,
                    },
                    {
                        "path": "ydb/tests/olap/scenario",
                        "test_count": 10,
                        "weight": 6000,
                    },
                ],
            }
            summary_path = Path(tmp) / "summary.json"
            summary_path.write_text(json.dumps(summary), encoding="utf-8")
            filtered_summary = Path(tmp) / "filtered.json"
            _run(
                "filter_suites_by_increment_graph.py",
                str(summary_path),
                str(FIXTURES / "graph_sample.json"),
                "--target-prefix",
                "ydb/tests/olap",
                "--summary-json",
                str(filtered_summary),
            )
            data = json.loads(filtered_summary.read_text(encoding="utf-8"))
            self.assertEqual(
                [suite["path"] for suite in data["suites"]],
                ["ydb/tests/olap/load"],
            )
            self.assertEqual(data["total_tests"], 5)
            self.assertEqual(data["total_weight"], 3000)

    def test_split_uses_suite_weights_for_balancing(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary = {
                "size_weights": {"small": 60, "medium": 600},
                "suites": [
                    {
                        "path": "ydb/tests/foo",
                        "small_test_count": 0,
                        "medium_test_count": 100,
                        "test_count": 100,
                        "weight": 60000,
                    },
                    {
                        "path": "ydb/tests/bar",
                        "small_test_count": 10,
                        "medium_test_count": 0,
                        "test_count": 10,
                        "weight": 600,
                    },
                ],
            }
            tests_path = Path(tmp) / "tests.txt"
            summary_path = Path(tmp) / "summary.json"
            tests_path.write_text("ydb/tests/foo\nydb/tests/bar\n", encoding="utf-8")
            summary_path.write_text(json.dumps(summary), encoding="utf-8")
            plan_path = Path(tmp) / "plan.json"
            _run(
                "split_test_shards.py",
                "--tests-file",
                str(tests_path),
                "--suite-weights-file",
                str(summary_path),
                "--shard-count",
                "2",
                "-o",
                str(plan_path),
            )
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            self.assertEqual(plan["total_tests"], 110)
            self.assertEqual(plan["total_weight"], 60600)
            loads = [shard["balance_weight"] for shard in plan["shards"]]
            self.assertEqual(loads, [60000, 600])

    def test_apply_history_replaces_size_weight_with_duration(self):
        if str(SHARDING_DIR) not in sys.path:
            sys.path.insert(0, str(SHARDING_DIR))
        if str(SHARDING_DIR.parent) not in sys.path:
            sys.path.insert(0, str(SHARDING_DIR.parent))
        import apply_history_suite_weights as ahsw  # noqa: E402

        with tempfile.TemporaryDirectory() as tmp:
            summary_path = Path(tmp) / "summary.json"
            _run(
                "extract_suites_from_ya_test_list.py",
                str(FIXTURES / "ya_test_list_olap.log"),
                "--target-prefix",
                "ydb/tests/olap",
                "--summary-json",
                str(summary_path),
            )
            summary = json.loads(summary_path.read_text(encoding="utf-8"))

            def fake_p50(paths, _days, _build, _branch, **kwargs):
                return {path: 100.0 for path in paths}

            with patch.object(ahsw, "get_suite_duration_p50", side_effect=fake_p50):
                updated = ahsw.apply_history_weights(
                    summary,
                    list_log=FIXTURES / "ya_test_list_olap.log",
                    target_prefix="ydb/tests/olap",
                    allowed_sizes={"small", "medium"},
                    build_type="relwithdebinfo",
                    branch="main",
                    days_back=3,
                )

            self.assertEqual(updated["weighting"]["history_suite_count"], len(updated["suites"]))
            self.assertEqual(updated["weighting"]["size_fallback_suite_count"], 0)
            self.assertEqual(updated["total_weight"], float(len(updated["suites"])) * 100.0)
            self.assertTrue(all(suite["weight_source"] == "history" for suite in updated["suites"]))

    def test_build_shard_blacklist_complement(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "plan.json"
            _run(
                "split_test_shards.py",
                "--tests-file",
                str(FIXTURES / "tests.txt"),
                "--shard-count",
                "2",
                "-o",
                str(plan_path),
            )
            blacklist_path = Path(tmp) / "blacklist.yaml"
            _run(
                "build_shard_blacklist.py",
                "--plan",
                str(plan_path),
                "--shard-id",
                "0",
                "-o",
                str(blacklist_path),
            )
            text = blacklist_path.read_text(encoding="utf-8")
            self.assertIn("path: ydb/tests/bar", text)
            self.assertNotIn("test_filter:", text)


class MergeReportsLatestWinsTest(unittest.TestCase):
    def test_later_attempt_overrides_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            try1 = Path(tmp) / "try_1.json"
            try2 = Path(tmp) / "try_2.json"
            try1.write_text(
                json.dumps(
                    {
                        "results": [
                            {"path": "ydb/a", "name": "T", "subtest_name": "ok", "type": "test", "status": "OK"},
                            {"path": "ydb/a", "name": "T", "subtest_name": "flaky", "type": "test", "status": "FAILED"},
                        ]
                    }
                ),
                encoding="utf-8",
            )
            try2.write_text(
                json.dumps(
                    {
                        "results": [
                            {"path": "ydb/a", "name": "T", "subtest_name": "flaky", "type": "test", "status": "OK"},
                        ]
                    }
                ),
                encoding="utf-8",
            )
            out = Path(tmp) / "final.json"
            _run("merge_build_reports.py", "--latest-wins", "-o", str(out), str(try1), str(try2))
            final = json.loads(out.read_text(encoding="utf-8"))
            statuses = {r["subtest_name"]: r["status"] for r in final["results"]}
            self.assertEqual(statuses, {"ok": "OK", "flaky": "OK"})


class ChooseShardCountTest(unittest.TestCase):
    @staticmethod
    def _choose(total_weight_min: float, **kwargs) -> int:
        count, _ = choose_shard_count(total_weight_min * 60.0, threads=1, **kwargs)
        return count

    def test_light_run_stays_single(self):
        self.assertEqual(self._choose(30), 1)
        self.assertEqual(self._choose(59.9), 1)

    def test_tiers_by_estimated_duration(self):
        self.assertEqual(self._choose(90), 4)
        self.assertEqual(self._choose(150), 8)
        self.assertEqual(self._choose(250), 12)

    def test_peak_cap_limits_heavy_runs(self):
        self.assertEqual(self._choose(250, is_peak=True), 4)
        self.assertEqual(self._choose(90, is_peak=True), 4)
        self.assertEqual(self._choose(30, is_peak=True), 1)

    def test_max_shards_bound(self):
        self.assertEqual(self._choose(250, max_shards=6), 6)

    def test_estimate_critical_path_minutes(self):
        self.assertAlmostEqual(estimate_critical_path_minutes([5200.0, 4800.0], 52), 5200.0 / 60.0 / 52.0)

    def test_enrich_plan_timing_estimate(self):
        plan = {
            "shard_count": 2,
            "total_weight": 10000.0,
            "shards": [{"balance_weight": 5200.0}, {"balance_weight": 4800.0}],
        }
        enrich_plan_timing_estimate(plan, 52)
        self.assertEqual(plan["estimate_threads"], 52)
        self.assertEqual(plan["estimated_max_shard_weight_sec"], 5200.0)
        self.assertEqual(plan["estimated_critical_path_min"], round(5200.0 / 60.0 / 52.0, 1))
        self.assertEqual(plan["estimated_single_job_min"], round(10000.0 / 60.0 / 52.0, 1))

    def test_render_shard_plan_summary_includes_estimate(self):
        plan = enrich_plan_timing_estimate(
            {
                "plan_mode": "increment_graph",
                "shard_count": 2,
                "total_graph_nodes": 10,
                "total_weight": 10000.0,
                "shards": [
                    {"id": 0, "result_node_count": 5, "balance_weight": 5200.0, "sample_paths": ["a"]},
                    {"id": 1, "result_node_count": 5, "balance_weight": 4800.0, "sample_paths": ["b"]},
                ],
            },
            52,
        )
        text = render_shard_plan_summary(plan)
        self.assertIn("Estimated wall time (slowest shard)", text)
        self.assertIn("Monolith equivalent", text)

    def test_cli_reads_summary_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary = Path(tmp) / "list_summary.json"
            # 150 estimated minutes on 52 threads -> 8 shards off-peak.
            summary.write_text(json.dumps({"total_weight": 150 * 60 * 52}), encoding="utf-8")
            result = _run(
                "choose_shard_count.py",
                str(summary),
                "--now-utc-hour",
                "3",
            )
            self.assertEqual(result.stdout.strip(), "8")
            result_peak = _run(
                "choose_shard_count.py",
                str(summary),
                "--now-utc-hour",
                "12",
            )
            self.assertEqual(result_peak.stdout.strip(), "4")


class FilterGraphForShardTest(unittest.TestCase):
    @staticmethod
    def _two_shard_plan() -> dict:
        return {
            "shard_count": 2,
            "shards": [
                {"id": 0, "tests": ["ydb/tests/foo"]},
                {"id": 1, "tests": ["ydb/tests/bar"]},
            ],
        }

    def test_assignments_partition_result_and_keep_components(self):
        graph = load_graph(FIXTURES / "graph_for_shard_filter.json")
        assignments = assign_result_uids_to_shards(self._two_shard_plan(), graph)
        self.assertEqual(len(assignments), len(graph["result"]))
        self.assertEqual(set(assignments), set(graph["result"]))
        # foo component (tests + build-a) stays on one shard.
        foo_nodes = {"test-a1", "test-a2", "build-a"}
        bar_nodes = {"test-b1", "build-b"}
        self.assertEqual(len({assignments[uid] for uid in foo_nodes}), 1)
        self.assertEqual(len({assignments[uid] for uid in bar_nodes}), 1)
        self.assertNotEqual(assignments["test-a1"], assignments["test-b1"])

    def test_filter_for_shard_writes_subset_result(self):
        graph = load_graph(FIXTURES / "graph_for_shard_filter.json")
        plan = self._two_shard_plan()
        assignments = assign_result_uids_to_shards(plan, graph)
        plan["uid_assignments"] = assignments
        plan["plan_mode"] = "increment_graph"
        shard_id = assignments["test-a1"]
        filtered, _, _ = filter_for_shard(plan, graph, shard_id)
        self.assertLess(len(filtered["result"]), len(graph["result"]))
        self.assertEqual(len(filtered["graph"]), len(graph["graph"]))
        for uid in filtered["result"]:
            self.assertEqual(assignments[uid], shard_id)

    def test_split_graph_result_fixture(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "plan.json"
            _run(
                "split_graph_result.py",
                str(FIXTURES / "graph_for_shard_filter.json"),
                "--shard-count",
                "2",
                "-o",
                str(plan_path),
            )
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            self.assertEqual(plan["plan_mode"], "increment_graph")
            self.assertEqual(plan["shard_count"], 2)
            self.assertEqual(len(plan["uid_assignments"]), plan["total_graph_nodes"])
            self.assertIn("estimated_critical_path_min", plan)
            self.assertEqual(plan["estimate_threads"], 52)

    def test_split_graph_result_balances_large_connected_component(self):
        graph = {
            "conf": {},
            "inputs": {},
            "result": [f"test-{idx}" for idx in range(80)] + ["build-a", "test-b1", "build-b"],
            "graph": [
                {
                    "uid": f"test-{idx}",
                    "node-type": "test",
                    "target_properties": {"module_dir": f"ydb/tests/synthetic_balance/chunk{idx}"},
                    "deps": ["build-a"],
                }
                for idx in range(80)
            ]
            + [
                {"uid": "build-a", "target_properties": {"module_dir": "ydb/tests/foo/support"}, "deps": []},
                {
                    "uid": "test-b1",
                    "node-type": "test",
                    "target_properties": {"module_dir": "ydb/tests/bar"},
                    "deps": ["build-b"],
                },
                {"uid": "build-b", "target_properties": {"module_dir": "ydb/tests/bar/deps"}, "deps": []},
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            graph_path = Path(tmp) / "graph.json"
            plan_path = Path(tmp) / "plan.json"
            graph_path.write_text(json.dumps(graph), encoding="utf-8")
            _run(
                "split_graph_result.py",
                str(graph_path),
                "--shard-count",
                "4",
                "--default-weight-sec",
                "10",
                "-o",
                str(plan_path),
            )
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            loads = [shard["result_node_count"] for shard in plan["shards"]]
            self.assertEqual(sum(loads), len(graph["result"]))
            self.assertLess(max(loads), 60)
            self.assertGreater(min(loads), 10)

    def test_cli_filters_graph_for_shard(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "plan.json"
            plan_path.write_text(json.dumps(self._two_shard_plan()), encoding="utf-8")
            out = Path(tmp) / "graph_shard_0.json"
            _run(
                "filter_graph_for_shard.py",
                "--graph",
                str(FIXTURES / "graph_for_shard_filter.json"),
                "--plan",
                str(plan_path),
                "--shard-id",
                "0",
                "-o",
                str(out),
            )
            filtered = json.loads(out.read_text(encoding="utf-8"))
            self.assertTrue(filtered["result"])
            self.assertLess(len(filtered["result"]), 5)


class RenderArtifactsNavTest(unittest.TestCase):
    def test_render_root_index_lists_top_level_folders(self):
        with tempfile.TemporaryDirectory() as tmp:
            merged = Path(tmp)
            (merged / "try_1").mkdir()
            (merged / "try_2").mkdir()
            (merged / "try_1" / "shard_0.json").write_text("{}", encoding="utf-8")
            (merged / "try_1" / "shard_1.json").write_text("{}", encoding="utf-8")
            html = render_nav_html(
                base_url="https://s3.example/run/x86-64",
                tries_dir=merged,
                include_build=True,
                include_plan=True,
            )
            self.assertIn('href="build/"', html)
            self.assertIn('href="plan/"', html)
            self.assertIn('href="shard_0/"', html)
            self.assertIn('href="shard_1/"', html)
            self.assertIn('href="try_1/"', html)
            self.assertIn('href="try_2/"', html)
            self.assertIn('href="final/"', html)
            self.assertIn("<table", html)
            self.assertNotIn("<h2>Merged</h2>", html)

    def test_render_root_index_without_plan(self):
        html = render_nav_html(
            base_url="https://s3.example/run/x86-64",
            tries_dir=None,
            include_build=True,
            include_plan=False,
        )
        self.assertIn('href="build/"', html)
        self.assertNotIn('href="plan/"', html)


class RunnerCapacityTest(unittest.TestCase):
    CONFIG = {
        "quotas": {"vcpu": 5400, "ram_gb": 23000, "instances": 110, "nrd_ssd_gb": 200000},
        "reserved": {"vcpu": 200, "ram_gb": 600, "instances": 22, "nrd_ssd_gb": 16000},
        "headroom_fraction": 1.0,
        "footprints": {
            "build-preset-relwithdebinfo": {"vcpu": 64, "ram_gb": 256, "nrd_ssd_gb": 2417},
            "build-preset-release-asan": {"vcpu": 96, "ram_gb": 288, "nrd_ssd_gb": 2417},
        },
        "default_footprint": {"vcpu": 96, "ram_gb": 320, "nrd_ssd_gb": 2417},
        "saturated_min_shards": 2,
    }

    def test_empty_pool_is_limited_by_smallest_quota(self):
        max_new, details = compute_max_new_runners(
            Counter(), "build-preset-relwithdebinfo", self.CONFIG
        )
        # vcpu: 5200/64=81.25, ram: 22400/256=87.5, ssd: 184000/2417=76.1,
        # instances: 88 -> ssd is the binding constraint.
        self.assertEqual(max_new, 76)
        self.assertEqual(details["used"]["instances"], 0)

    def test_busy_pool_reduces_capacity(self):
        demand = Counter(
            {"build-preset-relwithdebinfo": 40, "build-preset-release-asan": 25}
        )
        max_new, details = compute_max_new_runners(
            demand, "build-preset-relwithdebinfo", self.CONFIG
        )
        # vcpu used: 40*64 + 25*96 = 4960 -> free 240 -> 3 runners; this is
        # now the binding constraint (ssd free 26935 -> 11, instances 23).
        self.assertEqual(max_new, 3)
        self.assertEqual(details["used"]["vcpu"], 4960)

    def test_saturated_pool_returns_zero_before_floor(self):
        demand = Counter({"build-preset-release-asan": 60})
        max_new, _ = compute_max_new_runners(
            demand, "build-preset-relwithdebinfo", self.CONFIG
        )
        self.assertEqual(max_new, 0)

    def test_unknown_label_uses_default_footprint(self):
        demand = Counter({"build-preset-release-msan": 10})
        _, details = compute_max_new_runners(
            demand, "build-preset-relwithdebinfo", self.CONFIG
        )
        self.assertEqual(details["used"]["vcpu"], 960)


if __name__ == "__main__":
    unittest.main()
