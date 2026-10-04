#!/usr/bin/env python3
"""Partition invariants for shard_graph.py. No ya and no network."""
from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

import shard_graph


def _node(
    uid: str,
    *,
    deps: list[str] | None = None,
    size: str | None = None,
    timeout: str | None = None,
    cpu: int | str = 1,
    path: str | None = None,
    node_type: str | None = "test",
    cmd_tokens: list[str] | None = None,
) -> dict:
    cmd_args: list[str] = list(cmd_tokens or [])
    if size:
        cmd_args.extend(["--test-size", size])
    if timeout:
        cmd_args.extend(["--timeout", timeout])
    node: dict = {
        "uid": uid,
        "deps": list(deps or []),
        "cmds": [{"cmd_args": cmd_args}] if cmd_args else [],
        "requirements": {"cpu": cpu},
    }
    if node_type:
        node["node-type"] = node_type
    if path:
        node["target_properties"] = {"module_dir": path}
    return node


def _graph(nodes: list[dict], result: list[str] | None = None) -> dict:
    return {"result": result if result is not None else [node["uid"] for node in nodes], "graph": nodes}


class ShardPlanTest(unittest.TestCase):
    def test_every_result_uid_is_assigned_once(self) -> None:
        nodes = [
            _node("test-a", size="small", path="ydb/a"),
            _node("test-b", size="medium", path="ydb/b"),
            _node("test-c", size="small", path="ydb/c"),
            _node("lib", node_type=None, path="ydb/lib"),
        ]
        graph = _graph(nodes)
        plan = shard_graph.build_plan(graph, 2, threads=52)
        assignments = plan["uid_assignments"]
        self.assertEqual(set(assignments), {"test-a", "test-b", "test-c", "lib"})
        self.assertEqual(len(assignments), 4)
        self.assertEqual(sum(shard["result_node_count"] for shard in plan["shards"]), 4)
        self.assertTrue(all(0 <= shard_id < plan["shard_count"] for shard_id in assignments.values()))

    def test_assignment_is_stable_regardless_of_result_order(self) -> None:
        nodes = [
            _node("test-a", size="small"),
            _node("test-b", size="large", cpu="all"),
            _node("test-c", size="medium"),
        ]
        forward = shard_graph.build_plan(_graph(nodes), 2, threads=8)
        backward = shard_graph.build_plan(
            _graph(nodes, result=["test-c", "test-b", "test-a"]),
            2,
            threads=8,
        )
        again = shard_graph.build_plan(_graph(nodes), 2, threads=8)
        self.assertEqual(forward["uid_assignments"], backward["uid_assignments"])
        self.assertEqual(forward["uid_assignments"], again["uid_assignments"])

    def test_shard_count_is_capped_to_result_nodes(self) -> None:
        graph = _graph([_node("test-a", size="small"), _node("test-b", size="small")])
        plan = shard_graph.build_plan(graph, 10, threads=4)
        self.assertEqual(plan["requested_shard_count"], 10)
        self.assertEqual(plan["shard_count"], 2)
        self.assertEqual(sorted(plan["uid_assignments"].values()), [0, 1])

    def test_single_shard_keeps_every_node(self) -> None:
        graph = _graph([_node("test-a"), _node("test-b")])
        plan = shard_graph.build_plan(graph, 1)
        self.assertEqual(plan["shard_count"], 1)
        self.assertEqual(set(plan["uid_assignments"].values()), {0})

    def test_empty_result_is_an_error(self) -> None:
        with self.assertRaises(ValueError):
            shard_graph.build_plan(_graph([], result=[]), 2)

    def test_heavy_node_is_not_packed_with_every_light_node(self) -> None:
        nodes = [_node("test-heavy", size="large", cpu="all", path="ydb/heavy")]
        nodes.extend(_node(f"test-light-{index}", size="small", path=f"ydb/light/{index}") for index in range(6))
        plan = shard_graph.build_plan(_graph(nodes), 2, threads=52)
        heavy_shard = plan["uid_assignments"]["test-heavy"]
        light_shards = {plan["uid_assignments"][f"test-light-{index}"] for index in range(6)}
        self.assertTrue(light_shards - {heavy_shard}, "light nodes must use the other shard")

    def test_chunk_deps_increase_weight(self) -> None:
        leaf = _node("test-leaf", size="small", timeout="60")
        chunks = [
            _node("chunk-0", cmd_tokens=["run_test"], node_type=None),
            _node("chunk-1", cmd_tokens=["run_test"], node_type=None),
        ]
        suite = _node("test-suite", size="small", timeout="60", deps=["chunk-0", "chunk-1"])
        graph = _graph([suite, *chunks, leaf], result=["test-suite", "test-leaf"])
        nodes_by_uid = shard_graph.graph_nodes_by_uid(graph)
        result_set = set(shard_graph.result_uids(graph))
        suite_weight, _, suite_units = shard_graph.uid_weight(
            "test-suite", nodes_by_uid["test-suite"], nodes_by_uid, result_set, {}, 4
        )
        leaf_weight, _, leaf_units = shard_graph.uid_weight(
            "test-leaf", nodes_by_uid["test-leaf"], nodes_by_uid, result_set, {}, 4
        )
        self.assertEqual(suite_units, 2)
        self.assertEqual(leaf_units, 1)
        self.assertEqual(suite_weight, leaf_weight * 2)

    def test_missing_size_defaults_to_small(self) -> None:
        node = _node("test-nosize")
        size = shard_graph.resolve_node_test_size("test-nosize", node, {})
        self.assertEqual(size, "small")


class FilterGraphTest(unittest.TestCase):
    def _sample(self) -> dict:
        return _graph(
            [
                _node("test-a", deps=["lib"], size="small", path="ydb/a"),
                _node("test-b", deps=["lib"], size="large", path="ydb/b"),
                _node("lib", deps=["src"], node_type=None),
                _node("src", node_type=None),
                _node("other", node_type=None),
            ],
            result=["test-a", "test-b", "lib"],
        )

    def test_filter_keeps_dep_closure_and_only_assigned_results(self) -> None:
        graph = self._sample()
        plan = shard_graph.build_plan(graph, 2, threads=4)
        shard_id = plan["uid_assignments"]["test-a"]
        allowed = {uid for uid, assigned in plan["uid_assignments"].items() if assigned == shard_id}
        filtered = shard_graph.filter_graph_result(graph, allowed)
        self.assertEqual(set(filtered["result"]), allowed)
        kept = {node["uid"] for node in filtered["graph"]}
        self.assertIn("lib", kept)
        self.assertIn("src", kept)
        self.assertNotIn("other", kept)
        # A result UID owned by the other shard is not a goal of this one.
        other_goals = set(shard_graph.result_uids(graph)) - allowed
        self.assertTrue(other_goals.isdisjoint(filtered["result"]))

    def test_union_of_filtered_results_is_the_original_result(self) -> None:
        graph = self._sample()
        plan = shard_graph.build_plan(graph, 3, threads=4)
        union: set[str] = set()
        for shard in plan["shards"]:
            allowed = shard_graph.assignments_for_shard(plan, graph, shard["id"])
            filtered = shard_graph.filter_graph_result(graph, allowed)
            union.update(filtered["result"])
        self.assertEqual(union, set(shard_graph.result_uids(graph)))

    def test_context_keeps_only_shard_test_uids(self) -> None:
        context = {"tests": {"test-a": "keep", "test-b": "drop", "other": "drop"}}
        filtered = shard_graph.filter_context_tests(context, {"test-a"})
        self.assertEqual(filtered["tests"], {"test-a": "keep"})

    def test_filter_rejects_a_plan_that_drops_a_result_uid(self) -> None:
        graph = self._sample()
        plan = shard_graph.build_plan(graph, 2, threads=4)
        del plan["uid_assignments"]["test-b"]
        with self.assertRaises(ValueError):
            shard_graph.assignments_for_shard(plan, graph, 0)


class CliTest(unittest.TestCase):
    def test_plan_and_filter_round_trip(self) -> None:
        graph = _graph(
            [
                _node("test-a", deps=["lib"], size="small"),
                _node("test-b", size="medium"),
                _node("lib", node_type=None),
            ]
        )
        context = {"tests": {"test-a": "A", "test-b": "B"}}
        script = Path(shard_graph.__file__)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            graph_path = root / "graph.json"
            context_path = root / "context.json"
            plan_path = root / "plan.json"
            graph_path.write_text(json.dumps(graph), encoding="utf-8")
            context_path.write_text(json.dumps(context), encoding="utf-8")
            plan_run = subprocess.run(
                [
                    "python3",
                    str(script),
                    "plan",
                    "--graph",
                    str(graph_path),
                    "--context",
                    str(context_path),
                    "--shard-count",
                    "2",
                    "--threads",
                    "8",
                    "-o",
                    str(plan_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(plan_run.returncode, 0, plan_run.stderr)
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            covered: set[str] = set()
            for shard_id in range(plan["shard_count"]):
                out = root / f"shard_{shard_id}.json"
                ctx_out = root / f"ctx_{shard_id}.json"
                filt = subprocess.run(
                    [
                        "python3",
                        str(script),
                        "filter",
                        "--graph",
                        str(graph_path),
                        "--context",
                        str(context_path),
                        "--plan",
                        str(plan_path),
                        "--shard-id",
                        str(shard_id),
                        "-o",
                        str(out),
                        "--context-output",
                        str(ctx_out),
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(filt.returncode, 0, filt.stderr)
                covered.update(json.loads(out.read_text(encoding="utf-8"))["result"])
            self.assertEqual(covered, {"test-a", "test-b", "lib"})


if __name__ == "__main__":
    unittest.main()
