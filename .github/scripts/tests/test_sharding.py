#!/usr/bin/env python3
"""Unit tests for PR-check sharding helpers."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "sharding"
SHARDING_DIR = Path(__file__).resolve().parent / "sharding"


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
            self.assertNotIn("ydb/tests/olap", suites)
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


if __name__ == "__main__":
    unittest.main()
