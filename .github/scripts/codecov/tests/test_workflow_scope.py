#!/usr/bin/env python3
"""Guard the workflow entry points as well as the selected coverage matrix."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[4]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
PRODUCT_PATHS = [
    "ydb/public/sdk/cpp/**",
    "ydb/apps/ydb/**",
    "ydb/public/lib/ydb_cli/**",
    "ydb/tests/functional/ydb_cli/**",
    "ydb/library/workload/**",
    ".github/actions/run_clang_codecov/**",
    ".github/actions/setup_ci_ydb_service_account_key_file_credentials/**",
    ".github/scripts/codecov/**",
    "!.github/scripts/codecov/tests/**",
    ".github/workflows/cpp_codecov.yml",
    ".github/codecov.yml",
]
HELPER_PATHS = [
    ".github/actions/run_clang_codecov/**",
    ".github/scripts/codecov/**",
    ".github/workflows/cpp_codecov.yml",
    ".github/workflows/cpp_codecov_checks.yml",
    ".github/codecov.yml",
]


def load_workflow(name: str) -> dict:
    # Ruby is already used by helper CI. Normalize YAML 1.1's boolean `on` key
    # before JSON serialization; YAML 1.2 parsers can instead preserve "on".
    result = subprocess.run(
        [
            "ruby", "-rjson", "-ryaml", "-e",
            "config = YAML.safe_load(File.read(ARGV.fetch(0)), aliases: false); "
            "config['on'] = config.delete(true) if config.key?(true); "
            "puts JSON.generate(config)",
            str(WORKFLOWS / name),
        ],
        check=True, text=True, capture_output=True,
    )
    return json.loads(result.stdout)


def path_is_selected(path: str, patterns: list[str]) -> bool:
    """Evaluate our deliberately restricted literal/trailing-/** allowlists."""
    selected = False
    for pattern in patterns:
        exclude = pattern.startswith("!")
        glob = pattern[1:] if exclude else pattern
        if glob.endswith("/**"):
            prefix = glob[:-2]
            if any(char in prefix for char in "*?[]+!"):
                raise AssertionError(f"Unsupported broad workflow pattern: {pattern}")
            matches = path.startswith(prefix)
        else:
            if any(char in glob for char in "*?[]+!"):
                raise AssertionError(f"Unsupported broad workflow pattern: {pattern}")
            matches = path == glob
        if matches:
            selected = not exclude
    return selected


class WorkflowScopeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.product = load_workflow("cpp_codecov.yml")
        cls.helpers = load_workflow("cpp_codecov_checks.yml")

    def run_decide_step(self, paths: list[str], event: str = "pull_request_target") -> dict:
        """Execute the real YAML's shell selector, without building anything."""
        decide = next(
            step for step in self.product["jobs"]["detect"]["steps"]
            if step.get("id") == "decide"
        )
        with tempfile.TemporaryDirectory() as directory:
            changed = Path(directory) / "changed-files.txt"
            output = Path(directory) / "github-output.txt"
            changed.write_text("\n".join(paths) + "\n", encoding="utf-8")
            subprocess.run(
                ["bash", "-c", decide["run"]],
                cwd=REPO_ROOT,
                env={
                    **os.environ,
                    "EVENT_NAME": event,
                    "CHANGED_FILES_FILE": str(changed),
                    "GITHUB_OUTPUT": str(output),
                },
                check=True, text=True, capture_output=True,
            )
            return dict(line.split("=", 1) for line in output.read_text().splitlines())

    def test_product_events_have_exact_main_only_path_filters(self) -> None:
        events = self.product["on"]
        self.assertEqual(set(events), {"pull_request_target", "push"})
        for event, trigger in events.items():
            with self.subTest(event=event):
                self.assertEqual(trigger["branches"], ["main"])
                self.assertEqual(trigger["paths"], PRODUCT_PATHS)
                self.assertNotIn("paths-ignore", trigger)
                self.assertNotIn("branches-ignore", trigger)
        self.assertEqual(
            set(events["pull_request_target"]["types"]),
            {"opened", "synchronize", "reopened", "closed", "labeled"},
        )

    def test_helper_events_have_exact_infrastructure_only_filters(self) -> None:
        events = self.helpers["on"]
        self.assertEqual(set(events), {"pull_request", "push"})
        for event, trigger in events.items():
            with self.subTest(event=event):
                self.assertEqual(trigger, {"branches": ["main"], "paths": HELPER_PATHS})

    def test_unrelated_changes_start_neither_workflow_and_select_nothing(self) -> None:
        paths = [
            "README.md", "docs/coverage.md", "ydb/docs/en/core/reference/index.md",
            "ydb/core/base/appdata.cpp", "contrib/libs/openssl/ssl/ssl_lib.c",
            "util/system/file.cpp", "yql/essentials/core/yql_expr.cpp",
            ".github/workflows/build.yml", ".github/scripts/unrelated_helper.py",
            ".github/config/muted_ya.txt", ".github/codecov.yml.bak",
            "ydb/apps/ydbx/main.cpp", "ydb/public/sdk/cppx/client.cpp",
            "ydb/public/lib/ydb_clix/main.cpp", "ydb/library/workloads/main.cpp",
        ]
        for path in paths:
            for patterns in (PRODUCT_PATHS, HELPER_PATHS):
                with self.subTest(path=path, patterns=patterns):
                    self.assertFalse(path_is_selected(path, patterns))
        result = self.run_decide_step(paths)
        self.assertEqual(json.loads(result["matrix"]), [])
        self.assertEqual(result["should_run"], "false")

    def test_helper_only_changes_never_start_product_coverage(self) -> None:
        paths = [
            ".github/scripts/codecov/tests/test_helpers.py",
            ".github/scripts/codecov/tests/offline_workflow.yml",
            ".github/scripts/codecov/tests/fixtures/cli.cpp",
            ".github/workflows/cpp_codecov_checks.yml",
        ]
        for path in paths:
            with self.subTest(path=path):
                self.assertFalse(path_is_selected(path, PRODUCT_PATHS))
                self.assertTrue(path_is_selected(path, HELPER_PATHS))
        result = self.run_decide_step(paths)
        self.assertEqual(json.loads(result["matrix"]), [])
        self.assertEqual(result["should_run"], "false")

    def test_product_prs_select_only_changed_components(self) -> None:
        cases = [
            (["ydb/apps/ydb/main.cpp"], ["cli"]),
            (["ydb/public/lib/ydb_cli/commands.cpp"], ["cli"]),
            (["ydb/tests/functional/ydb_cli/test_cli.py"], ["cli"]),
            (["ydb/public/sdk/cpp/client.cpp"], ["cpp_sdk"]),
            (["ydb/library/workload/tpcc.cpp"], ["cli_workload"]),
            (["ydb/apps/ydb/main.cpp", "ydb/public/sdk/cpp/client.cpp"], ["cli", "cpp_sdk"]),
        ]
        for paths, expected in cases:
            with self.subTest(paths=paths):
                for path in paths:
                    self.assertTrue(path_is_selected(path, PRODUCT_PATHS))
                    self.assertFalse(path_is_selected(path, HELPER_PATHS))
                # Adding documentation/helper tests cannot broaden the matrix.
                result = self.run_decide_step(paths + [
                    "README.md", ".github/scripts/codecov/tests/test_helpers.py",
                ])
                self.assertEqual(json.loads(result["matrix"]), expected)
                self.assertEqual(result["should_run"], "true")

    def test_runtime_infrastructure_selects_all_components(self) -> None:
        paths = [
            ".github/actions/run_clang_codecov/action.yaml",
            ".github/actions/setup_ci_ydb_service_account_key_file_credentials/action.yaml",
            ".github/scripts/codecov/run_coverage.py",
            ".github/workflows/cpp_codecov.yml", ".github/codecov.yml",
        ]
        for path in paths:
            with self.subTest(path=path):
                self.assertTrue(path_is_selected(path, PRODUCT_PATHS))
                result = self.run_decide_step([path])
                self.assertEqual(json.loads(result["matrix"]), ["cli", "cli_workload", "cpp_sdk"])

    def test_relevant_main_push_is_full_but_product_job_requires_a_nonempty_matrix(self) -> None:
        path = "ydb/apps/ydb/main.cpp"
        self.assertTrue(path_is_selected(path, self.product["on"]["push"]["paths"]))
        result = self.run_decide_step([path], event="push")
        self.assertEqual(json.loads(result["matrix"]), ["cli", "cli_workload", "cpp_sdk"])
        condition = self.product["jobs"]["coverage"]["if"]
        self.assertIn("needs.detect.result == 'success'", condition)
        self.assertIn("needs.detect.outputs.should_run == 'true'", condition)


if __name__ == "__main__":
    unittest.main()
