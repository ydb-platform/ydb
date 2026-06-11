#!/usr/bin/env python3
"""Unit tests for PR-check sharding helpers."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

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
