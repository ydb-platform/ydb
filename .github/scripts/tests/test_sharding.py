#!/usr/bin/env python3
"""Unit tests for PR-check sharding helpers."""
from __future__ import annotations

import io
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "sharding"
SHARDING_DIR = Path(__file__).resolve().parent / "sharding"

if str(SHARDING_DIR) not in sys.path:
    sys.path.insert(0, str(SHARDING_DIR))

from filter import filter_for_shard  # noqa: E402
from merge import (  # noqa: E402
    merge_reports,
    merge_reports_latest_wins,
)
from plan import (  # noqa: E402
    WEIGHT_MODE_HISTORY,
    WEIGHT_MODE_TIMEOUT_BUDGET,
    build_plan,
    choose_shard_count,
    cpu_slots,
    enrich_plan_timing_estimate,
    estimate_critical_path_minutes,
    extract_node_path,
    load_graph,
    min_shards_for_wall_budget,
    plan_uid_weights,
    print_summary,
)


def _run(script: str, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SHARDING_DIR / script), *args],
        check=True,
        capture_output=True,
        text=True,
    )


class MergeReportsTest(unittest.TestCase):
    def test_merge_build_reports(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "merged.json"
            _run(
                "merge.py",
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
            _run("merge.py", "-o", str(out), str(shard_a), str(shard_b))
            merged = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(len(merged["results"]), 1)
            self.assertEqual(merged["results"][0]["status"], "FAILED")

    def test_merge_reports_worst_status_wins_in_memory(self):
        row = {"path": "p", "name": "n", "subtest_name": "s", "type": "test"}
        merged = merge_reports(
            [
                {"results": [{**row, "status": "OK"}]},
                {"results": [{**row, "status": "ERROR"}]},
            ]
        )
        self.assertEqual(len(merged["results"]), 1)
        self.assertEqual(merged["results"][0]["status"], "ERROR")

    def test_merge_reports_latest_wins_in_memory(self):
        row = {"path": "p", "name": "n", "subtest_name": "s", "type": "test"}
        merged = merge_reports_latest_wins(
            [
                {"results": [{**row, "status": "FAILED"}]},
                {"results": [{**row, "status": "OK"}]},
            ]
        )
        self.assertEqual(len(merged["results"]), 1)
        self.assertEqual(merged["results"][0]["status"], "OK")


class MergeReportsLatestWinsCLITest(unittest.TestCase):
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
            _run("merge.py", "--latest-wins", "-o", str(out), str(try1), str(try2))
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

    def test_soft_profile_tiers(self):
        self.assertEqual(self._choose(90, profile="soft"), 2)
        self.assertEqual(self._choose(150, profile="soft"), 4)
        self.assertEqual(self._choose(250, profile="soft"), 8)
        self.assertEqual(self._choose(30, profile="soft"), 1)

    def test_peak_cap_limits_heavy_runs(self):
        self.assertEqual(self._choose(250, is_peak=True), 4)
        self.assertEqual(self._choose(90, is_peak=True), 4)
        self.assertEqual(self._choose(30, is_peak=True), 1)
        # soft peak: heavy would be 8, capped to 4
        self.assertEqual(self._choose(250, profile="soft", is_peak=True), 4)

    def test_max_shards_bound(self):
        self.assertEqual(self._choose(250, max_shards=6), 6)

    def test_min_shards_for_wall_budget(self):
        self.assertEqual(min_shards_for_wall_budget(0, threads=1), 1)
        self.assertEqual(min_shards_for_wall_budget(240 * 60, threads=1), 1)
        self.assertEqual(min_shards_for_wall_budget(241 * 60, threads=1), 2)
        self.assertEqual(min_shards_for_wall_budget(1000 * 60, threads=1), 5)

    def test_wall_budget_floor_overrides_capacity_and_peak(self):
        # 1000 min single-job -> wall floor ceil(1000/240)=5.
        self.assertEqual(self._choose(1000, max_shards=2), 5)
        self.assertEqual(self._choose(1000, is_peak=True), 5)
        # Below the 4h budget the old peak/capacity caps still apply.
        self.assertEqual(self._choose(250, is_peak=True), 4)
        self.assertEqual(self._choose(250, max_shards=2), 2)

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


class PrintSummaryTest(unittest.TestCase):
    def test_print_summary_includes_estimate_and_table(self):
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
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            print_summary(plan)
        text = buffer.getvalue()
        self.assertIn("Estimated wall time (slowest shard)", text)
        self.assertIn("Monolith equivalent", text)
        self.assertIn("| Shard | Graph nodes | Weight | Sample paths |", text)


class PlanUidWeightsTest(unittest.TestCase):
    def test_extract_node_path_prefers_kv_path_over_clang_format_input(self):
        node = {
            "uid": "test-lint",
            "kv": {"path": "yql/essentials/udfs/common/compress_base/clang_format"},
            "inputs": [
                "$(SOURCE_ROOT)/tools/cpp_style_checker/wrapper.py",
                "$(SOURCE_ROOT)/yql/essentials/.clang-format",
                "$(SOURCE_ROOT)/yql/essentials/udfs/common/compress_base",
            ],
            "outputs": [
                "$(BUILD_ROOT)/yql/essentials/udfs/common/compress_base/test-results/clang_format/meta.json",
            ],
        }
        self.assertEqual(
            extract_node_path(node),
            "yql/essentials/udfs/common/compress_base",
        )

    def test_plan_uid_weights_timeout_budget_mode(self):
        nodes = {
            "test-acc": {
                "uid": "test-acc",
                "node-type": "test",
                "target_properties": {"module_dir": "ydb/tests/foo"},
                "cmds": [{"cmd_args": ["results_accumulator"]}],
                "deps": ["test-chunk0", "test-chunk1", "build-dep"],
            },
            "test-chunk0": {
                "uid": "test-chunk0",
                "node-type": "test",
                "cmds": [{"cmd_args": ["run_test", "--test-size", "large", "--timeout", "3600"]}],
            },
            "test-chunk1": {
                "uid": "test-chunk1",
                "node-type": "test",
                "cmds": [{"cmd_args": ["run_test", "--test-size", "large", "--timeout", "3600"]}],
            },
            "build-dep": {"uid": "build-dep", "deps": []},
            "test-leaf": {
                "uid": "test-leaf",
                "node-type": "test",
                "target_properties": {"module_dir": "ydb/tests/bar"},
                "cmds": [{"cmd_args": ["run_test", "--test-size", "small"]}],
            },
        }
        weights, stats = plan_uid_weights(
            ["test-acc", "test-leaf"],
            nodes,
            {},
            weight_mode=WEIGHT_MODE_TIMEOUT_BUDGET,
        )
        self.assertEqual(weights["test-acc"], 2 * 3600.0)
        self.assertEqual(weights["test-leaf"], 60.0)
        self.assertEqual(stats["mode"], "graph_uid_timeout_budget_lpt")
        self.assertEqual(stats["timeout_budget_units"], 3)
        self.assertEqual(stats["size_large_uid_count"], 1)
        self.assertEqual(stats["size_small_uid_count"], 1)

    def test_plan_uid_weights_splits_history_by_matched_suite(self):
        nodes = {
            "test-a": {
                "uid": "test-a",
                "node-type": "test",
                "target_properties": {"module_dir": "ydb/tests/foo"},
                "cmds": [{"cmd_args": ["run_test", "--test-size", "medium"]}],
            },
            "test-b": {
                "uid": "test-b",
                "node-type": "test",
                "target_properties": {"module_dir": "ydb/tests/foo/child"},
                "cmds": [{"cmd_args": ["run_test", "--test-size", "medium"]}],
            },
            "test-large": {
                "uid": "test-large",
                "node-type": "test",
                "target_properties": {"module_dir": "ydb/tests/bar"},
                "cmds": [{"cmd_args": ["run_test", "--test-size", "large"]}],
            },
        }
        weights, stats = plan_uid_weights(
            ["test-a", "test-b", "test-large"],
            nodes,
            {"ydb/tests/foo": 100.0},
        )
        self.assertEqual(weights["test-a"], 50.0)
        self.assertEqual(weights["test-b"], 50.0)
        self.assertEqual(weights["test-large"], 3600.0)
        self.assertEqual(stats["history_uid_count"], 2)
        self.assertEqual(stats["size_large_uid_count"], 1)

    def test_plan_uid_weights_history_owned_by_heaviest_size(self):
        """Large co-located with small/non-test must not re-slice the same p90."""
        nodes = {
            "test-large": {
                "uid": "test-large",
                "node-type": "test",
                "target_properties": {"module_dir": "ydb/tests/olap/large"},
                "cmds": [{"cmd_args": ["run_test", "--test-size", "large"]}],
            },
            "test-small": {
                "uid": "test-small",
                "node-type": "test",
                "target_properties": {"module_dir": "ydb/tests/olap/large"},
                "cmds": [{"cmd_args": ["run_test", "--test-size", "small"]}],
            },
            "support-peer": {
                "uid": "support-peer",
                "target_properties": {"module_dir": "ydb/tests/olap/large"},
            },
        }
        with_large, stats_large = plan_uid_weights(
            ["test-large", "test-small", "support-peer"],
            nodes,
            {"ydb/tests/olap/large": 1000.0},
            weight_mode=WEIGHT_MODE_HISTORY,
        )
        self.assertEqual(with_large["test-large"], 1000.0)
        self.assertEqual(with_large["test-small"], 60.0)
        self.assertEqual(with_large["support-peer"], 60.0)
        self.assertEqual(stats_large["history_uid_count"], 1)
        self.assertEqual(sum(with_large.values()), 1120.0)

        without_large, stats_small = plan_uid_weights(
            ["test-small", "support-peer"],
            nodes,
            {"ydb/tests/olap/large": 1000.0},
            weight_mode=WEIGHT_MODE_HISTORY,
        )
        self.assertEqual(without_large["test-small"], 1000.0)
        self.assertEqual(without_large["support-peer"], 60.0)
        self.assertEqual(stats_small["history_uid_count"], 1)
        self.assertLess(sum(without_large.values()), sum(with_large.values()))

    def test_cpu_slots_reads_any_requirement_value(self):
        cases = [
            ({}, 1),
            ({"requirements": {"cpu": 1}}, 1),
            ({"requirements": {"cpu": 2}}, 2),
            ({"requirements": {"cpu": 3}}, 3),
            ({"requirements": {"cpu": 4}}, 4),
            ({"requirements": {"cpu": 8}}, 8),
            ({"requirements": {"cpu": "16"}}, 16),
            ({"requirements": {"cpu": 52}}, 52),
            ({"requirements": {"cpu": 100}}, 52),
            ({"requirements": {"cpu": "all"}}, 52),
            ({"requirements": {"cpu": 0}}, 1),
            ({"requirements": {"cpu": "nope"}}, 1),
        ]
        for node, expected in cases:
            self.assertEqual(cpu_slots(node, threads=52), expected, msg=node)

    def test_plan_uid_weights_scales_by_any_cpu(self):
        nodes = {
            "test-2cpu": {
                "uid": "test-2cpu",
                "node-type": "test",
                "target_properties": {"module_dir": "ydb/tests/a"},
                "requirements": {"cpu": 2},
                "cmds": [{"cmd_args": ["run_test", "--test-size", "medium"]}],
            },
            "test-8cpu": {
                "uid": "test-8cpu",
                "node-type": "test",
                "target_properties": {"module_dir": "ydb/tests/b"},
                "requirements": {"cpu": 8},
                "cmds": [{"cmd_args": ["run_test", "--test-size", "medium"]}],
            },
            "test-all-cpu": {
                "uid": "test-all-cpu",
                "node-type": "test",
                "target_properties": {"module_dir": "ydb/tests/c"},
                "requirements": {"cpu": "all"},
                "cmds": [{"cmd_args": ["run_test", "--test-size", "medium"]}],
            },
        }
        history = {
            "ydb/tests/a": 100.0,
            "ydb/tests/b": 100.0,
            "ydb/tests/c": 100.0,
        }
        weights, stats = plan_uid_weights(
            ["test-2cpu", "test-8cpu", "test-all-cpu"],
            nodes,
            history,
            weight_mode=WEIGHT_MODE_HISTORY,
            threads=52,
        )
        self.assertEqual(weights["test-2cpu"], 200.0)
        self.assertEqual(weights["test-8cpu"], 800.0)
        self.assertEqual(weights["test-all-cpu"], 100.0 * 52)
        self.assertTrue(stats["cpu_scaled"])
        self.assertEqual(stats["threads"], 52)

        budget, _ = plan_uid_weights(
            ["test-2cpu", "test-8cpu"],
            nodes,
            {},
            weight_mode=WEIGHT_MODE_TIMEOUT_BUDGET,
            threads=52,
        )
        self.assertEqual(budget["test-2cpu"], 1200.0)
        self.assertEqual(budget["test-8cpu"], 4800.0)

    def test_critical_path_uses_slot_seconds_over_threads(self):
        # 5200 slot-seconds on 52 threads -> 100/60 minutes.
        self.assertAlmostEqual(estimate_critical_path_minutes([5200.0], 52), 100.0 / 60.0)
        plan = {
            "shards": [{"id": 0, "balance_weight": 5200.0}],
            "total_weight": 5200.0,
        }
        enrich_plan_timing_estimate(plan, 52)
        self.assertEqual(plan["estimate_threads"], 52)
        self.assertAlmostEqual(plan["estimated_critical_path_min"], round(100.0 / 60.0, 1))


class BuildPlanAndFilterTest(unittest.TestCase):
    def test_build_plan_partitions_result_uids_across_shards(self):
        graph = load_graph(FIXTURES / "graph_for_shard_filter.json")
        plan = build_plan(graph, 2, duration_p90={})
        assignments = plan["uid_assignments"]
        self.assertEqual(set(assignments), set(graph["result"]))
        self.assertEqual(plan["total_graph_nodes"], len(graph["result"]))
        self.assertEqual(plan["shard_count"], len({sid for sid in assignments.values()}))

    def test_filter_for_shard_writes_subset_result(self):
        graph = load_graph(FIXTURES / "graph_for_shard_filter.json")
        plan = build_plan(graph, 2, duration_p90={})
        assignments = plan["uid_assignments"]
        # Pick a shard that has at least one node.
        shard_id = next(iter(sorted({sid for sid in assignments.values()})))
        filtered, _, returned = filter_for_shard(plan, graph, shard_id)
        self.assertLessEqual(len(filtered["result"]), len(graph["result"]))
        self.assertGreater(len(filtered["result"]), 0)
        self.assertEqual(len(filtered["graph"]), len(graph["graph"]))
        for uid in filtered["result"]:
            self.assertEqual(returned[uid], shard_id)

    def test_filter_for_shard_requires_uid_assignments(self):
        graph = load_graph(FIXTURES / "graph_for_shard_filter.json")
        with self.assertRaises(ValueError):
            filter_for_shard({"shard_count": 2, "shards": []}, graph, 0)

    def test_split_graph_result_fixture(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan_path = Path(tmp) / "plan.json"
            _run(
                "plan.py",
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
                "plan.py",
                str(graph_path),
                "--shard-count",
                "4",
                "--small-weight-sec",
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
            _run(
                "plan.py",
                str(FIXTURES / "graph_for_shard_filter.json"),
                "--shard-count",
                "2",
                "-o",
                str(plan_path),
            )
            out = Path(tmp) / "graph_shard_0.json"
            _run(
                "filter.py",
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


if __name__ == "__main__":
    unittest.main()
