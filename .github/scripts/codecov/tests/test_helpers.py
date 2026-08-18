#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

TEST_DIR = Path(__file__).resolve().parent
CODECOV_DIR = TEST_DIR.parent
GITHUB_DIR = CODECOV_DIR.parent.parent
sys.path.insert(0, str(CODECOV_DIR))

from codecov_suites import SUITES, suites_from_paths
from export_coverage_lcov import filter_lcov, parse_ya_llvm_cov_cmd, path_matches_prefixes


class CodecovSuitesTest(unittest.TestCase):
    def test_product_paths_select_only_the_affected_suites(self) -> None:
        self.assertEqual(suites_from_paths(["ydb/public/sdk/cpp/client/ydb_driver.cpp"]), ["cpp_sdk"])
        self.assertEqual(suites_from_paths(["ydb/apps/ydb/commands/ydb.cpp"]), ["cli"])
        self.assertEqual(
            suites_from_paths(["ydb/tests/functional/ydb_cli/test_cli.py"]),
            ["cli"],
        )
        self.assertEqual(
            suites_from_paths(["ydb/library/workload/workload.cpp"]),
            ["cli_workload"],
        )

    def test_multiple_product_paths_are_combined(self) -> None:
        self.assertEqual(
            suites_from_paths(
                [
                    "ydb/public/sdk/cpp/client/ydb_driver.cpp",
                    "ydb/library/workload/workload.cpp",
                ]
            ),
            ["cli_workload", "cpp_sdk"],
        )

    def test_unrelated_paths_select_nothing(self) -> None:
        self.assertEqual(suites_from_paths(["ydb/core/base/appdata.h"]), [])

    def test_coverage_infrastructure_selects_every_suite(self) -> None:
        expected = sorted(SUITES)
        shared_paths = [
            ".github/actions/run_clang_codecov/action.yaml",
            ".github/actions/setup_ci_ydb_service_account_key_file_credentials/action.yaml",
            ".github/scripts/codecov/export_coverage_lcov.py",
            ".github/workflows/cpp_codecov.yml",
            ".github/workflows/cpp_codecov_checks.yml",
            ".github/codecov.yml",
        ]
        for path in shared_paths:
            with self.subTest(path=path):
                self.assertEqual(suites_from_paths([path]), expected)

        self.assertEqual(
            suites_from_paths([".github/scripts/codecov/future_helper.py"]),
            expected,
        )

    def test_similarly_named_files_are_not_coverage_infrastructure(self) -> None:
        self.assertEqual(suites_from_paths([".github/codecov.yml.bak"]), [])
        self.assertEqual(suites_from_paths([".github/workflows/cpp_codecov.yml.disabled"]), [])
        self.assertEqual(
            suites_from_paths([".github/scripts/codecov-extra/helper.py"]),
            [],
        )

    def test_helper_unit_tests_do_not_select_product_coverage(self) -> None:
        self.assertEqual(
            suites_from_paths([".github/scripts/codecov/tests/test_helpers.py"]),
            [],
        )

class DetectCodecovMatrixTest(unittest.TestCase):
    def run_detector(self, *args: str) -> tuple[dict[str, str], str]:
        with tempfile.NamedTemporaryFile() as output:
            env = os.environ.copy()
            env["GITHUB_OUTPUT"] = output.name
            proc = subprocess.run(
                [sys.executable, str(CODECOV_DIR / "detect_codecov_matrix.py"), *args],
                check=True,
                text=True,
                capture_output=True,
                env=env,
            )
            output.seek(0)
            values = {}
            for line in output.read().decode().splitlines():
                name, value = line.split("=", 1)
                values[name] = value
            return values, proc.stderr

    def test_paths_mode(self) -> None:
        values, _ = self.run_detector(
            "--changed-files",
            "ydb/apps/ydb/main.cpp\nydb/public/sdk/cpp/client.cpp",
        )
        self.assertEqual(json.loads(values["matrix"]), ["cli", "cpp_sdk"])
        self.assertEqual(values["should_run"], "true")

    def test_rename_out_keeps_the_suite_selected(self) -> None:
        values, _ = self.run_detector(
            "--changed-files",
            "docs/moved-client.cpp\nydb/public/sdk/cpp/client.cpp",
        )
        self.assertEqual(json.loads(values["matrix"]), ["cpp_sdk"])
        self.assertEqual(values["should_run"], "true")


class ExportCoverageLcovTest(unittest.TestCase):
    def test_parse_llvm_cov_command_skips_option_values(self) -> None:
        log = (
            "Executing: ['/tools/bin/llvm-cov', 'export', '--instr-profile', "
            "'/tmp/coverage.profdata', '--ignore-filename-regex', '/(ut|tests)/', "
            "'/build/bin/ydb', '--object', '/build/bin/ydb-cli', "
            "'-object=/build/bin/ydb-workload']"
        )
        llvm_cov, objects = parse_ya_llvm_cov_cmd(log)
        self.assertEqual(llvm_cov, "/tools/bin/llvm-cov")
        self.assertEqual(
            objects,
            ["/build/bin/ydb", "/build/bin/ydb-cli", "/build/bin/ydb-workload"],
        )

    def test_prefix_matching_uses_path_boundaries(self) -> None:
        prefixes = ["ydb/public/sdk/cpp/"]
        self.assertTrue(path_matches_prefixes("/repo/ydb/public/sdk/cpp/client.cpp", prefixes))
        self.assertFalse(path_matches_prefixes("/repo/ydb/public/sdk/cppx/client.cpp", prefixes))

    def test_filter_lcov_keeps_product_and_drops_tests(self) -> None:
        lcov = """TN:
SF:/repo/ydb/public/sdk/cpp/client.cpp
DA:1,1
end_of_record
TN:
SF:/repo/ydb/public/sdk/cpp/tests/client_ut.cpp
DA:1,1
end_of_record
TN:
SF:/repo/ydb/core/base/appdata.cpp
DA:1,1
end_of_record
"""
        filtered = filter_lcov(lcov, ["ydb/public/sdk/cpp/"])
        self.assertIn("client.cpp", filtered)
        self.assertNotIn("client_ut.cpp", filtered)
        self.assertNotIn("appdata.cpp", filtered)
        self.assertEqual(filtered.count("end_of_record"), 1)

    def test_export_refuses_a_missing_instrumented_binary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ya_output = Path(directory)
            report = ya_output / "coverage.report"
            report.mkdir()
            (report / "coverage.profdata").touch()
            missing = ya_output / "missing-llvm-object"
            (ya_output / "build_clang_coverage_report.log").write_text(
                f"Executing: ['/tools/bin/llvm-cov', 'export', '{missing}']\n",
                encoding="utf-8",
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(CODECOV_DIR / "export_coverage_lcov.py"),
                    "--suite",
                    "cli",
                    "--ya-output",
                    str(ya_output),
                    "--output",
                    str(ya_output / "coverage.lcov"),
                ],
                check=False,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("Refusing to publish incomplete coverage", proc.stderr)

    def test_export_refuses_an_empty_filtered_report(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ya_output = Path(directory)
            report = ya_output / "coverage.report"
            report.mkdir()
            (report / "coverage.profdata").touch()
            llvm_object = ya_output / "instrumented-binary"
            llvm_object.touch()
            tool = ya_output / "bin" / "llvm-cov"
            tool.parent.mkdir()
            tool.write_text(
                "#!/usr/bin/env sh\n"
                "printf 'TN:\\nSF:/repo/ydb/core/base.cpp\\nDA:1,1\\nend_of_record\\n'\n",
                encoding="utf-8",
            )
            tool.chmod(0o755)
            (ya_output / "build_clang_coverage_report.log").write_text(
                f"Executing: ['{tool}', 'export', '{llvm_object}']\n",
                encoding="utf-8",
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(CODECOV_DIR / "export_coverage_lcov.py"),
                    "--suite",
                    "cli",
                    "--ya-output",
                    str(ya_output),
                    "--output",
                    str(ya_output / "coverage.lcov"),
                    "--llvm-cov",
                    str(tool),
                ],
                check=False,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("Filtered LCOV is empty", proc.stderr)


class GenerateCoverageLandingTest(unittest.TestCase):
    def test_output_is_deterministic_and_contains_only_requested_suites(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory) / "first.html"
            second = Path(directory) / "second.html"
            command = [
                sys.executable,
                str(CODECOV_DIR / "generate_coverage_landing.py"),
                "--title",
                "Coverage",
                "--suite",
                "cli=./cli/index.html",
            ]
            subprocess.run([*command, "--output", str(first)], check=True)
            subprocess.run([*command, "--output", str(second)], check=True)
            self.assertEqual(first.read_text(), second.read_text())
            self.assertIn("./cli/index.html", first.read_text())
            self.assertNotIn("cpp_sdk", first.read_text())


class OverlayTrustedCodecovCiTest(unittest.TestCase):
    def test_overlay_replaces_the_entire_import_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            measured = root / "measured"
            trusted = root / "trusted"

            for action in (
                "run_clang_codecov",
                "setup_ci_ydb_service_account_key_file_credentials",
            ):
                measured_action = measured / ".github" / "actions" / action
                trusted_action = trusted / ".github" / "actions" / action
                measured_action.mkdir(parents=True)
                trusted_action.mkdir(parents=True)
                (measured_action / "untrusted.txt").write_text("untrusted\n")
                (trusted_action / "trusted.txt").write_text("trusted\n")

            measured_helpers = measured / ".github" / "scripts" / "codecov"
            trusted_helpers = trusted / ".github" / "scripts" / "codecov"
            measured_helpers.mkdir(parents=True)
            trusted_helpers.mkdir(parents=True)
            (measured_helpers / "json.py").write_text("raise RuntimeError('untrusted')\n")
            (trusted_helpers / "trusted_helper.py").write_text("TRUSTED = True\n")

            subprocess.run(
                ["bash", str(CODECOV_DIR / "overlay_trusted_codecov_ci.sh"), str(trusted)],
                cwd=measured,
                check=True,
            )

            self.assertFalse((measured_helpers / "json.py").exists())
            self.assertTrue((measured_helpers / "trusted_helper.py").is_file())
            for action in (
                "run_clang_codecov",
                "setup_ci_ydb_service_account_key_file_credentials",
            ):
                action_dir = measured / ".github" / "actions" / action
                self.assertFalse((action_dir / "untrusted.txt").exists())
                self.assertTrue((action_dir / "trusted.txt").is_file())


class WorkflowContractTest(unittest.TestCase):
    def test_only_ok_to_test_label_can_trigger_coverage(self) -> None:
        workflow = (
            GITHUB_DIR / "workflows" / "cpp_codecov.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("pull_request_target:", workflow)
        self.assertIn("branches: [main]", workflow)
        self.assertIn("types: [opened, synchronize, reopened, closed, labeled]", workflow)
        self.assertIn("github.event.label.name == 'ok-to-test'", workflow)
        self.assertIn('"!.github/scripts/codecov/tests/**"', workflow)
        self.assertIn("github.event.label.name != 'ok-to-test'", workflow)
        self.assertIn("format('-ignore-{0}', github.run_id)", workflow)
        self.assertIn(
            "github.event_name == 'pull_request_target' && github.event.action != 'labeled'",
            workflow,
        )
        self.assertNotIn("coverage/sdk", workflow)
        self.assertNotIn("coverage/cli", workflow)
        self.assertNotIn("coverage/workload", workflow)
        self.assertNotIn("coverage/all", workflow)
        self.assertNotIn("workflow_dispatch", workflow)

    def test_pr_authorization_is_event_and_sha_scoped(self) -> None:
        workflow = (
            GITHUB_DIR / "workflows" / "cpp_codecov.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("const permission = await getPermission(sender);", workflow)
        self.assertIn("if (!isWriter(permission))", workflow)
        self.assertIn("context.payload.pull_request.user.login", workflow)
        self.assertIn("context.payload.sender.login", workflow)
        self.assertNotIn("context.payload.pull_request.labels", workflow)
        self.assertIn("if (pr.head.sha !== eventPr.head.sha)", workflow)
        self.assertIn("steps.authorization.outputs.allowed == 'true'", workflow)

    def test_codecov_upload_uses_explicit_safe_arguments(self) -> None:
        action = (
            GITHUB_DIR / "actions" / "run_clang_codecov" / "action.yaml"
        ).read_text(encoding="utf-8")
        self.assertIn('--slug "${REPOSITORY_SLUG}"', action)
        self.assertIn('--sha "${COMMIT_SHA}"', action)
        self.assertIn("--disable-search", action)
        self.assertIn("--plugin noop", action)
        self.assertNotIn('-t "${CODECOV_TOKEN}"', action)
        self.assertNotIn("set -x", action)


if __name__ == "__main__":
    unittest.main()
