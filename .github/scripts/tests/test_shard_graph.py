#!/usr/bin/env python3
"""Partition invariants for shard_graph.py. No ya and no network."""
from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

import shard_graph
import shard_progress


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


def _progress_state(total: int = 4) -> dict:
    return shard_progress.empty_state("99", "relwithdebinfo", "ydb/", total)


class HostCountTest(unittest.TestCase):
    def _minutes(self, minutes: float, threads: int = 52) -> float:
        return minutes * 60.0 * threads

    def test_more_volume_asks_for_more_shards(self) -> None:
        threads = 52
        nodes = 100
        light = shard_graph.choose_host_count(
            result_nodes=nodes, total_weight_sec=self._minutes(30), threads=threads, free_runners=None
        )
        medium = shard_graph.choose_host_count(
            result_nodes=nodes, total_weight_sec=self._minutes(90), threads=threads, free_runners=None
        )
        heavy = shard_graph.choose_host_count(
            result_nodes=nodes, total_weight_sec=self._minutes(150), threads=threads, free_runners=None
        )
        self.assertEqual(light, 1)
        self.assertEqual(medium, 4)
        self.assertEqual(heavy, 8)
        self.assertLess(light, medium)
        self.assertLess(medium, heavy)

    def test_one_free_runner_forces_a_single_job(self) -> None:
        huge = self._minutes(250)
        for free in (0, 1):
            chosen = shard_graph.choose_host_count(
                result_nodes=80, total_weight_sec=huge, threads=52, free_runners=free
            )
            self.assertEqual(chosen, 1)

    def test_cap_is_16_when_there_are_more_nodes(self) -> None:
        chosen = shard_graph.choose_host_count(
            result_nodes=100,
            total_weight_sec=self._minutes(4000),
            threads=52,
            free_runners=None,
        )
        self.assertEqual(chosen, 16)

    def test_fewer_nodes_than_desired_shards_shrinks_the_count(self) -> None:
        chosen = shard_graph.choose_host_count(
            result_nodes=3,
            total_weight_sec=self._minutes(4000),
            threads=52,
            free_runners=None,
        )
        self.assertEqual(chosen, 3)

    def test_explicit_count_beats_auto(self) -> None:
        chosen = shard_graph.choose_host_count(
            result_nodes=50,
            total_weight_sec=self._minutes(4000),
            threads=52,
            free_runners=1,
            explicit=2,
        )
        self.assertEqual(chosen, 2)

    def test_quota_math_reports_one_free_slot(self) -> None:
        config = {
            "quotas": {"vcpu": 10, "ram_gb": 10, "nrd_ssd_gb": 10, "instances": 2},
            "reserved": {},
            "headroom_fraction": 1.0,
            "footprints": {
                "build-preset-relwithdebinfo": {"vcpu": 5, "ram_gb": 5, "nrd_ssd_gb": 5},
            },
            "default_footprint": {"vcpu": 5, "ram_gb": 5, "nrd_ssd_gb": 5},
        }
        free = shard_graph.compute_max_new_runners(
            shard_graph.Counter({"build-preset-relwithdebinfo": 1}),
            "build-preset-relwithdebinfo",
            config,
        )
        self.assertEqual(free, 1)
        chosen = shard_graph.choose_host_count(
            result_nodes=40,
            total_weight_sec=self._minutes(250),
            threads=52,
            free_runners=free,
        )
        self.assertEqual(chosen, 1)

    def test_next_link_and_busy_pages_are_both_counted(self) -> None:
        header = '<https://example.test/runs?page=2>; rel="next", <https://example.test/runs?page=1>; rel="prev"'
        self.assertEqual(shard_graph.next_link(header), "https://example.test/runs?page=2")
        self.assertEqual(shard_graph.next_link(""), "")
        page_one = {"jobs": [{"status": "in_progress", "labels": ["self-hosted", "build-preset-relwithdebinfo"]}]}
        page_two = {"jobs": [{"status": "queued", "labels": ["build-preset-release-asan"]}]}
        demand = shard_graph.busy_labels_in_jobs(page_one)
        demand.update(shard_graph.busy_labels_in_jobs(page_two))
        self.assertEqual(demand["build-preset-relwithdebinfo"], 1)
        self.assertEqual(demand["build-preset-release-asan"], 1)


class ShardProgressTest(unittest.TestCase):
    def test_eta_is_unknown_until_the_first_result_and_done_at_the_end(self) -> None:
        self.assertEqual(shard_progress.eta_label(0, 4, None), "unknown")
        self.assertEqual(shard_progress.eta_label(0, 4, 100), "unknown")
        # elapsed * (m - n) / n = 300s * 3 / 1 = 15m
        self.assertEqual(shard_progress.eta_label(1, 4, 300), "15m")
        self.assertEqual(shard_progress.eta_label(4, 4, 300), "done")

    def test_failure_is_visible_before_the_last_shard(self) -> None:
        state = shard_progress.apply_shard(
            _progress_state(),
            shard_id=0,
            result="failure",
            started_at="2026-10-04T12:00:00Z",
            finished_at="2026-10-04T12:05:00Z",
            job_url="https://example.test/job/0",
            log_prefix="shard_0",
            failed_tests=["ydb/a/unittest/Foo"],
            run_url="https://example.test/run/99",
        )
        body = shard_progress.render_comment(state, "2026-10-04T12:05:00Z")
        self.assertIn("**Progress:** 1/4", body)
        self.assertIn("**ETA:** 15m", body)
        self.assertIn("**Status:** running", body)
        self.assertIn("shard 0 **failure**", body)
        self.assertIn("`ydb/a/unittest/Foo`", body)
        self.assertIn("shard_0", body)
        self.assertIn("https://example.test/job/0", body)

    def test_merge_keeps_every_shard_and_uses_the_earliest_start(self) -> None:
        first = shard_progress.apply_shard(
            _progress_state(),
            shard_id=1,
            result="success",
            started_at="2026-10-04T12:02:00Z",
            finished_at="2026-10-04T12:06:00Z",
            job_url="https://example.test/job/1",
            log_prefix="shard_1",
            failed_tests=[],
            run_url="https://example.test/run/99",
        )
        second = shard_progress.apply_shard(
            _progress_state(),
            shard_id=0,
            result="failure",
            started_at="2026-10-04T12:00:00Z",
            finished_at="2026-10-04T12:10:00Z",
            job_url="https://example.test/job/0",
            log_prefix="shard_0",
            failed_tests=["ydb/b"],
            run_url="https://example.test/run/99",
        )
        merged = shard_progress.merge_states([first, second])
        self.assertEqual(shard_progress.received_count(merged), 2)
        self.assertEqual(merged["started_at"], "2026-10-04T12:00:00Z")
        body = shard_progress.render_comment(merged, "2026-10-04T12:10:00Z")
        self.assertIn("**Progress:** 2/4", body)
        self.assertIn("shard 0 **failure**", body)
        self.assertNotIn("**Status:** success", body)

    def test_full_set_is_the_final_summary(self) -> None:
        state = _progress_state(2)
        for shard_id, result in ((0, "success"), (1, "failure")):
            state = shard_progress.apply_shard(
                state,
                shard_id=shard_id,
                result=result,
                started_at="2026-10-04T12:00:00Z",
                finished_at="2026-10-04T12:04:00Z",
                job_url=f"https://example.test/job/{shard_id}",
                log_prefix=f"shard_{shard_id}",
                failed_tests=["ydb/bad"] if result == "failure" else [],
                run_url="https://example.test/run/99",
            )
        body = shard_progress.render_comment(state, "2026-10-04T12:04:00Z")
        self.assertIn("**Progress:** 2/2", body)
        self.assertIn("**ETA:** done", body)
        self.assertIn("**Status:** failure", body)
        parsed = shard_progress.parse_state(body)
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(set(parsed["shards"]), {"0", "1"})

    def test_comment_update_retries_on_etag_conflict(self) -> None:
        header = shard_progress.marker("99", "relwithdebinfo")
        store = _ConflictStore()
        initial = shard_progress.apply_shard(
            _progress_state(),
            shard_id=0,
            result="success",
            started_at="2026-10-04T12:00:00Z",
            finished_at="2026-10-04T12:05:00Z",
            job_url="https://example.test/job/0",
            log_prefix="shard_0",
            failed_tests=[],
            run_url="https://example.test/run/99",
        )
        store.create(shard_progress.render_comment(initial, "2026-10-04T12:05:00Z"))
        incoming = shard_progress.apply_shard(
            _progress_state(),
            shard_id=1,
            result="failure",
            started_at="2026-10-04T12:00:00Z",
            finished_at="2026-10-04T12:10:00Z",
            job_url="https://example.test/job/1",
            log_prefix="shard_1",
            failed_tests=["ydb/late"],
            run_url="https://example.test/run/99",
        )
        body = shard_progress.sync_comment(store, header, incoming, "2026-10-04T12:10:00Z")
        self.assertGreaterEqual(store.conflicts, 1)
        self.assertIn("**Progress:** 2/4", body)
        self.assertIn("`ydb/late`", body)
        self.assertIn("shard 0", body)
        self.assertEqual(len(store.rows), 1)

    def test_merge_uses_the_fresh_comment_not_the_stale_list(self) -> None:
        header = shard_progress.marker("99", "relwithdebinfo")
        listed = shard_progress.apply_shard(
            _progress_state(),
            shard_id=0,
            result="success",
            started_at="2026-10-04T12:00:00Z",
            finished_at="2026-10-04T12:05:00Z",
            job_url="https://example.test/job/0",
            log_prefix="shard_0",
            failed_tests=[],
            run_url="https://example.test/run/99",
        )
        fresh = shard_progress.apply_shard(
            listed,
            shard_id=2,
            result="failure",
            started_at="2026-10-04T12:00:00Z",
            finished_at="2026-10-04T12:06:00Z",
            job_url="https://example.test/job/2",
            log_prefix="shard_2",
            failed_tests=["ydb/raced"],
            run_url="https://example.test/run/99",
        )
        store = _FreshBodyStore(
            listed_body=shard_progress.render_comment(listed, "2026-10-04T12:05:00Z"),
            fresh_body=shard_progress.render_comment(fresh, "2026-10-04T12:06:00Z"),
        )
        incoming = shard_progress.apply_shard(
            _progress_state(),
            shard_id=1,
            result="success",
            started_at="2026-10-04T12:00:00Z",
            finished_at="2026-10-04T12:07:00Z",
            job_url="https://example.test/job/1",
            log_prefix="shard_1",
            failed_tests=[],
            run_url="https://example.test/run/99",
        )
        body = shard_progress.sync_comment(store, header, incoming, "2026-10-04T12:07:00Z")
        self.assertIn("shard 2", body)
        self.assertIn("`ydb/raced`", body)
        self.assertIn("**Progress:** 3/4", body)

    def test_narrow_retry_keeps_only_the_failed_suite(self) -> None:
        graph = _graph(
            [
                _node("test-a", deps=["lib"], size="small", path="ydb/a"),
                _node("test-b", deps=["lib"], size="small", path="ydb/b"),
                _node("lib", deps=[], node_type=None),
            ],
            result=["test-a", "test-b"],
        )
        report = {"results": [{"status": "FAILED", "path": "ydb/b", "name": "T"}]}
        narrowed = shard_graph.narrow_graph_to_report(graph, report)
        self.assertEqual(narrowed["result"], ["test-b"])
        unchanged = shard_graph.narrow_graph_to_report(graph, {"results": []})
        self.assertEqual(unchanged["result"], ["test-a", "test-b"])
        by_uid = _graph(
            [_node("uid-only", size="small"), _node("other", size="small", path="ydb/other")],
            result=["uid-only", "other"],
        )
        narrowed_uid = shard_graph.narrow_graph_to_report(
            by_uid, {"results": [{"status": "FAILED", "uid": "uid-only"}]}
        )
        self.assertEqual(narrowed_uid["result"], ["uid-only"])

    def test_build_failure_is_not_reported_as_a_test_failure(self) -> None:
        build_state, test_state = shard_progress.aggregate_check_states(
            [{"build": "failure", "tests": ""}, {"build": "success", "tests": "success"}]
        )
        self.assertEqual(build_state, "failure")
        self.assertIsNone(test_state)
        build_state, test_state = shard_progress.aggregate_check_states(
            [{"build": "success", "tests": "failure"}]
        )
        self.assertEqual((build_state, test_state), ("success", "failure"))
        rows = shard_progress.rows_for_preset(
            [],
            [{"name": "Test relwithdebinfo shard 3", "conclusion": "failure"}],
            "relwithdebinfo",
        )
        self.assertEqual(rows, [{"build": "failure", "tests": ""}])

    def test_one_bad_plan_does_not_drop_the_other_preset(self) -> None:
        good = {
            "shard_count": 2,
            "threads": 52,
            "run": {
                "build_preset": "relwithdebinfo",
                "build_target": "ydb/",
                "test_size": "small,medium",
                "threads": 52,
            },
            "shards": [{"id": 0}, {"id": 1}],
        }
        single = {"shard_count": 1, "run": {"build_preset": "release-asan"}, "shards": [{"id": 0}]}
        broken = {"shard_count": 4, "shards": []}
        rows, errors = shard_graph.matrix_rows_from_plans(
            [("good", good), ("single", single), ("broken", broken), ("unreadable", {"_error": "bad json"})]
        )
        self.assertEqual([row["shard_id"] for row in rows], [0, 1])
        self.assertEqual({row["build_preset"] for row in rows}, {"relwithdebinfo"})
        self.assertEqual(rows[0]["build_target"], "ydb/")
        self.assertEqual(len(errors), 2)

    def test_comment_list_follows_the_next_link(self) -> None:
        self.assertEqual(
            shard_progress.next_link('<https://example.test/comments?page=2>; rel="next"'),
            "https://example.test/comments?page=2",
        )


class _MemComment:
    def __init__(self, comment_id: int, body: str, etag: str) -> None:
        self.id = comment_id
        self.body = body
        self.etag = etag


class _ConflictStore:
    def __init__(self) -> None:
        self.rows: dict[int, _MemComment] = {}
        self.next_id = 1
        self.conflicts_left = 1
        self.conflicts = 0

    def list_marker(self, header: str) -> list[_MemComment]:
        return [row for row in self.rows.values() if row.body.startswith(header)]

    def get(self, comment_id: int) -> _MemComment:
        row = self.rows[comment_id]
        return _MemComment(row.id, row.body, row.etag)

    def create(self, body: str) -> _MemComment:
        row = _MemComment(self.next_id, body, "etag-1")
        self.next_id += 1
        self.rows[row.id] = row
        return row

    def update(self, comment_id: int, body: str, etag: str) -> None:
        row = self.rows[comment_id]
        if self.conflicts_left and etag == row.etag:
            self.conflicts_left -= 1
            self.conflicts += 1
            row.etag = row.etag + "-stale"
            raise shard_progress.Conflict(str(comment_id))
        if etag != row.etag:
            raise shard_progress.Conflict(str(comment_id))
        row.body = body
        row.etag = row.etag + "-ok"

    def delete(self, comment_id: int) -> None:
        self.rows.pop(comment_id, None)


class _FreshBodyStore:
    """list_marker returns a stale body; get() returns the comment as it is now."""

    def __init__(self, listed_body: str, fresh_body: str) -> None:
        self.listed_body = listed_body
        self.fresh_body = fresh_body
        self.written = ""

    def list_marker(self, header: str) -> list[_MemComment]:
        if self.written:
            return [_MemComment(1, self.written, "etag-2")]
        return [_MemComment(1, self.listed_body, "")]

    def get(self, comment_id: int) -> _MemComment:
        body = self.written or self.fresh_body
        return _MemComment(comment_id, body, "etag-fresh")

    def create(self, body: str) -> _MemComment:
        raise AssertionError("create should not run when a comment already exists")

    def update(self, comment_id: int, body: str, etag: str) -> None:
        if etag != "etag-fresh":
            raise shard_progress.Conflict(str(comment_id))
        self.written = body

    def delete(self, comment_id: int) -> None:
        return None


if __name__ == "__main__":
    unittest.main()
