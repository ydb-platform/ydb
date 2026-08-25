#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

TEST_DIR = Path(__file__).resolve().parent
CODECOV_DIR = TEST_DIR.parent
GITHUB_DIR = CODECOV_DIR.parent.parent
sys.path.insert(0, str(CODECOV_DIR))

from codecov_suites import SUITES, suites_from_paths
from export_coverage_lcov import (
    filter_lcov,
    filter_suite_lcov,
    generate_html_report,
    is_ephemeral_ya_build_source,
    lcov_sources,
    parse_ya_llvm_cov_cmd,
    path_matches_prefixes,
)


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
        check_only_paths = [
            ".github/scripts/codecov/tests/test_helpers.py",
            ".github/workflows/cpp_codecov_checks.yml",
        ]
        for path in check_only_paths:
            with self.subTest(path=path):
                self.assertEqual(suites_from_paths([path]), [])

    def test_suite_instrumentation_regexes_are_path_bounded(self) -> None:
        cases = {
            "cpp_sdk": (
                ["ydb/public/sdk/cpp", "ydb/public/sdk/cpp/client"],
                ["ydb/public/sdk/cppx", "other/ydb/public/sdk/cpp"],
            ),
            "cli": (
                ["ydb/apps/ydb", "ydb/apps/ydb/commands", "ydb/public/lib/ydb_cli"],
                ["ydb/apps/ydbx", "ydb/public/lib/ydb_clix"],
            ),
            "cli_workload": (
                ["ydb/library/workload", "ydb/library/workload/tpcc"],
                ["ydb/library/workloads", "other/ydb/library/workload"],
            ),
        }
        for suite, (matches, misses) in cases.items():
            regexp = re.compile(SUITES[suite]["regexp"])
            for path in matches:
                with self.subTest(suite=suite, path=path, expected=True):
                    self.assertIsNotNone(regexp.match(path))
            for path in misses:
                with self.subTest(suite=suite, path=path, expected=False):
                    self.assertIsNone(regexp.match(path))

    def test_suite_owned_prefixes_are_exact_and_disjoint(self) -> None:
        expected = {
            "cpp_sdk": ["ydb/public/sdk/cpp/"],
            "cli": ["ydb/apps/ydb/", "ydb/public/lib/ydb_cli/"],
            "cli_workload": ["ydb/library/workload/"],
        }
        self.assertEqual(
            {name: cfg["lcov_prefixes"] for name, cfg in SUITES.items()},
            expected,
        )
        all_prefixes = [prefix for prefixes in expected.values() for prefix in prefixes]
        for index, left in enumerate(all_prefixes):
            for right in all_prefixes[index + 1 :]:
                with self.subTest(left=left, right=right):
                    self.assertFalse(left.startswith(right) or right.startswith(left))

    def test_cli_functional_tests_trigger_but_are_not_reported(self) -> None:
        path = "ydb/tests/functional/ydb_cli/test_cli.py"
        self.assertEqual(suites_from_paths([path]), ["cli"])
        self.assertFalse(path_matches_prefixes(path, SUITES["cli"]["lcov_prefixes"]))


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

    def test_all_mode_selects_every_suite(self) -> None:
        values, _ = self.run_detector("--all")
        self.assertEqual(json.loads(values["matrix"]), sorted(SUITES))
        self.assertEqual(values["should_run"], "true")

    def test_unrelated_paths_do_not_start_coverage(self) -> None:
        values, _ = self.run_detector(
            "--changed-files",
            "README.md\nydb/core/base/appdata.h",
        )
        self.assertEqual(json.loads(values["matrix"]), [])
        self.assertEqual(values["should_run"], "false")


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

    def test_suite_filter_drops_all_ya_build_root_sources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            existing = root / "ydb" / "library" / "workload" / "source.cpp"
            existing.parent.mkdir(parents=True)
            existing.touch()
            generated = (
                root
                / ".ya"
                / "build"
                / "build_root"
                / "hash"
                / "ydb"
                / "library"
                / "workload"
                / "generated.cpp"
            )
            generated.parent.mkdir(parents=True)
            generated.touch()
            missing_generated = generated.with_name("missing-generated.cpp")
            lcov = (
                f"TN:\nSF:{existing}\nDA:1,1\nend_of_record\n"
                f"TN:\nSF:{generated}\nDA:1,1\nend_of_record\n"
                f"TN:\nSF:{missing_generated}\nDA:1,1\nend_of_record\n"
            )
            filtered, dropped = filter_suite_lcov(
                lcov,
                SUITES["cli_workload"]["lcov_prefixes"],
            )
            self.assertEqual(lcov_sources(filtered), [str(existing)])
            self.assertEqual(dropped, [str(generated), str(missing_generated)])

    def test_suite_filter_refuses_a_missing_checked_in_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "ydb" / "apps" / "ydb" / "missing.cpp"
            lcov = f"TN:\nSF:{missing}\nDA:1,1\nend_of_record\n"
            with contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaisesRegex(SystemExit, "missing source files"):
                    filter_suite_lcov(lcov, SUITES["cli"]["lcov_prefixes"])

    def test_only_ya_build_root_paths_are_classified_as_ephemeral(self) -> None:
        self.assertTrue(
            is_ephemeral_ya_build_source(
                "/home/runner/.ya/build/build_root/hash/node/ydb/apps/ydb/generated.cpp"
            )
        )
        self.assertTrue(
            is_ephemeral_ya_build_source(
                r"C:\runner\.ya\build\build_root\hash\ydb\apps\ydb\generated.cpp"
            )
        )
        self.assertFalse(
            is_ephemeral_ya_build_source(
                "/home/runner/actions_runner/_work/ydb/ydb/ydb/apps/ydb/source.cpp"
            )
        )
        self.assertFalse(
            is_ephemeral_ya_build_source(
                "/home/runner/.ya/build/build_root-old/ydb/apps/ydb/generated.cpp"
            )
        )

    def test_each_suite_lcov_keeps_only_owned_sources(self) -> None:
        owned = {
            "cpp_sdk": [
                "/repo/ydb/public/sdk/cpp/client.cpp",
                "/repo/ydb/public/sdk/cpp/client.h",
            ],
            "cli": [
                "/repo/ydb/apps/ydb/commands.cpp",
                "/repo/ydb/public/lib/ydb_cli/commands.h",
            ],
            "cli_workload": [
                "/repo/ydb/library/workload/tpcc.cpp",
                "/repo/ydb/library/workload/tpcc.h",
            ],
        }
        owned_tests = {
            "cpp_sdk": "/repo/ydb/public/sdk/cpp/tests/client.cpp",
            "cli": "/repo/ydb/apps/ydb/commands_ut.cpp",
            "cli_workload": "/repo/ydb/library/workload/tests/tpcc.cpp",
        }
        common_unowned = [
            "/repo/ydb/core/base/appdata.cpp",
            "/repo/library/cpp/threading/future.cpp",
            "/repo/contrib/libs/abseil/status.cpp",
        ]
        lookalikes = {
            "cpp_sdk": "/repo/ydb/public/sdk/cppx/client.cpp",
            "cli": "/repo/ydb/public/lib/ydb_clix/commands.cpp",
            "cli_workload": "/repo/ydb/library/workloads/tpcc.cpp",
        }

        def record(path: str) -> str:
            return f"TN:\nSF:{path}\nDA:1,1\nend_of_record\n"

        for suite, owned_paths in owned.items():
            lcov = "".join(
                record(path)
                for path in [
                    *owned_paths,
                    owned_tests[suite],
                    lookalikes[suite],
                    *common_unowned,
                ]
            )
            filtered = filter_lcov(lcov, SUITES[suite]["lcov_prefixes"])
            with self.subTest(suite=suite):
                self.assertEqual(lcov_sources(filtered), owned_paths)

    def test_html_uses_the_same_filtered_sources_as_lcov(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ya_output = Path(directory)
            report = ya_output / "coverage.report"
            report.mkdir()
            (report / "coverage.profdata").touch()
            llvm_object = ya_output / "instrumented-binary"
            llvm_object.touch()
            calls = ya_output / "llvm-cov-calls.jsonl"
            owned_source = ya_output / "repo" / "ydb" / "apps" / "ydb" / "main.cpp"
            owned_source.parent.mkdir(parents=True)
            owned_source.touch()
            tool = ya_output / "bin" / "llvm-cov"
            tool.parent.mkdir()
            tool.write_text(
                "#!/usr/bin/env python3\n"
                "import json, os, pathlib, sys\n"
                f"calls = pathlib.Path({str(calls)!r})\n"
                "with calls.open('a', encoding='utf-8') as stream:\n"
                "    stream.write(json.dumps(sys.argv[1:]) + '\\n')\n"
                "if sys.argv[1] == 'export':\n"
                "    print(f'TN:\\nSF:{os.environ[\"FAKE_OWNED_SOURCE\"]}\\nDA:1,1\\nend_of_record')\n"
                "    print('TN:\\nSF:/repo/contrib/libs/abseil/status.cpp\\nDA:1,0\\nend_of_record')\n"
                "elif sys.argv[1] == 'show':\n"
                "    option = next(x for x in sys.argv if x.startswith('-output-dir='))\n"
                "    output = pathlib.Path(option.split('=', 1)[1])\n"
                "    output.mkdir(parents=True, exist_ok=True)\n"
                "    (output / 'index.html').write_text('filtered', encoding='utf-8')\n",
                encoding="utf-8",
            )
            tool.chmod(0o755)
            (ya_output / "build_clang_coverage_report.log").write_text(
                f"Executing: ['{tool}', 'export', '{llvm_object}']\n",
                encoding="utf-8",
            )
            lcov_output = ya_output / "coverage.lcov"
            html_output = ya_output / "coverage.filtered.report"
            fake_env = os.environ.copy()
            fake_env["FAKE_OWNED_SOURCE"] = str(owned_source)

            subprocess.run(
                [
                    sys.executable,
                    str(CODECOV_DIR / "export_coverage_lcov.py"),
                    "--suite",
                    "cli",
                    "--ya-output",
                    str(ya_output),
                    "--output",
                    str(lcov_output),
                    "--llvm-cov",
                    str(tool),
                ],
                check=True,
                text=True,
                capture_output=True,
                env=fake_env,
            )
            subprocess.run(
                [
                    sys.executable,
                    str(CODECOV_DIR / "export_coverage_lcov.py"),
                    "--suite",
                    "cli",
                    "--ya-output",
                    str(ya_output),
                    "--html-input",
                    str(lcov_output),
                    "--html-output",
                    str(html_output),
                    "--llvm-cov",
                    str(tool),
                ],
                check=True,
                text=True,
                capture_output=True,
                env=fake_env,
            )

            self.assertEqual(
                lcov_sources(lcov_output.read_text()),
                [str(owned_source)],
            )
            self.assertTrue((html_output / "index.html").is_file())
            invocations = [json.loads(line) for line in calls.read_text().splitlines()]
            show = next(args for args in invocations if args[0] == "show")
            source_index = show.index("-sources")
            self.assertEqual(show[source_index + 1 :], [str(owned_source)])
            self.assertNotIn("/repo/contrib/libs/abseil/status.cpp", show)

    def test_failed_html_generation_leaves_no_publishable_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "coverage.filtered.report"
            source = root / "repo" / "ydb" / "apps" / "ydb" / "main.cpp"
            source.parent.mkdir(parents=True)
            source.touch()
            completed = subprocess.CompletedProcess(
                args=["llvm-cov", "show"],
                returncode=1,
                stdout="partial output",
                stderr="show failed",
            )
            with mock.patch("export_coverage_lcov.subprocess.run", return_value=completed):
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                    io.StringIO()
                ):
                    with self.assertRaisesRegex(SystemExit, "HTML generation failed"):
                        generate_html_report(
                            "llvm-cov",
                            ["instrumented-object"],
                            root / "coverage.profdata",
                            f"TN:\nSF:{source}\nDA:1,1\nend_of_record\n",
                            output,
                        )
            self.assertFalse(output.exists())
            self.assertEqual(list(root.glob(".coverage.filtered.report.*")), [])

    def test_html_refuses_missing_filtered_sources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "coverage.filtered.report"
            with contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaisesRegex(SystemExit, "incomplete source set"):
                    generate_html_report(
                        "llvm-cov",
                        ["instrumented-object"],
                        root / "coverage.profdata",
                        "TN:\nSF:/missing/ydb/apps/ydb/main.cpp\nDA:1,1\nend_of_record\n",
                        output,
                    )
            self.assertFalse(output.exists())

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
    def test_codecov_flag_paths_match_suite_owned_prefixes(self) -> None:
        path = GITHUB_DIR / "codecov.yml"
        proc = subprocess.run(
            [
                "ruby",
                "-rjson",
                "-ryaml",
                "-e",
                "puts JSON.generate(YAML.safe_load(File.read(ARGV.fetch(0)), aliases: false))",
                str(path),
            ],
            check=True,
            text=True,
            capture_output=True,
        )
        config = json.loads(proc.stdout)
        self.assertEqual(config["codecov"]["strict_yaml_branch"], "default")
        self.assertIs(config["codecov"]["require_ci_to_pass"], True)
        self.assertIs(config["codecov"]["notify"]["wait_for_ci"], True)
        self.assertNotIn("after_n_builds", config["codecov"]["notify"])
        self.assertIs(config["github_checks"], False)
        self.assertIs(config["comment"]["hide_project_coverage"], True)
        self.assertEqual(
            config["comment"]["layout"],
            "condensed_header, condensed_files, condensed_footer",
        )
        self.assertNotIn("component_management", config)
        self.assertNotIn("show_carryforward_flags", config["comment"])
        self.assertIs(config["coverage"]["status"]["project"], False)
        self.assertIs(config["coverage"]["status"]["patch"], False)
        self.assertIs(
            config["flag_management"]["default_rules"]["carryforward"],
            False,
        )
        for item in config["flag_management"]["individual_flags"]:
            self.assertNotIn("carryforward", item)

        flags = {
            item["name"]: item["paths"]
            for item in config["flag_management"]["individual_flags"]
        }
        expected = {
            name: [f"{prefix}**" for prefix in cfg["lcov_prefixes"]]
            for name, cfg in SUITES.items()
        }
        self.assertEqual(flags, expected)

        expected_project_thresholds = {
            "cpp_sdk": "0.1%",
            "cli": "5%",
            "cli_workload": "5%",
        }
        for item in config["flag_management"]["individual_flags"]:
            statuses = {status["type"]: status for status in item["statuses"]}
            self.assertEqual(set(statuses), {"project", "patch"})
            self.assertEqual(statuses["project"]["target"], "auto")
            self.assertEqual(
                statuses["project"]["threshold"],
                expected_project_thresholds[item["name"]],
            )
            self.assertEqual(
                statuses["project"]["flag_coverage_not_uploaded_behavior"],
                "exclude",
            )
            self.assertEqual(statuses["patch"]["target"], "80%")
            self.assertIs(statuses["patch"]["informational"], True)
            self.assertEqual(
                statuses["patch"]["flag_coverage_not_uploaded_behavior"],
                "exclude",
            )

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

    def test_production_triggers_cover_only_runtime_paths(self) -> None:
        path = GITHUB_DIR / "workflows" / "cpp_codecov.yml"
        proc = subprocess.run(
            [
                "ruby",
                "-rjson",
                "-ryaml",
                "-e",
                "puts JSON.generate(YAML.safe_load(File.read(ARGV.fetch(0)), aliases: false))",
                str(path),
            ],
            check=True,
            text=True,
            capture_output=True,
        )
        config = json.loads(proc.stdout)
        expected_paths = [
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
        triggers = config["true"]
        self.assertEqual(triggers["pull_request_target"]["paths"], expected_paths)
        self.assertEqual(triggers["push"]["paths"], expected_paths)
        self.assertEqual(triggers["pull_request_target"]["branches"], ["main"])
        self.assertEqual(triggers["push"]["branches"], ["main"])

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

    def test_pr_changed_files_are_pinned_to_the_authorized_sha(self) -> None:
        workflow = (
            GITHUB_DIR / "workflows" / "cpp_codecov.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("PR_HEAD_SHA: ${{ steps.pr.outputs.sha }}", workflow)
        self.assertIn("PR_BASE_SHA: ${{ steps.pr.outputs.base_sha }}", workflow)
        self.assertIn('"+refs/pull/${PR_NUMBER}/head:${local_head_ref}"', workflow)
        self.assertIn('if [ "$fetched_head" != "$PR_HEAD_SHA" ]', workflow)
        self.assertIn('"$PR_BASE_SHA...$PR_HEAD_SHA"', workflow)
        self.assertIn("git -c core.quotePath=false diff", workflow)
        self.assertIn("--name-only --no-renames", workflow)
        self.assertNotIn("pulls/${PR_NUMBER}/files", workflow)

    def test_trusted_helpers_never_come_from_a_stale_pr_base(self) -> None:
        workflow = (
            GITHUB_DIR / "workflows" / "cpp_codecov.yml"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "Checkout trusted detection helpers from workflow source",
            workflow,
        )
        self.assertIn(
            "Checkout trusted coverage helpers from workflow source",
            workflow,
        )
        self.assertEqual(workflow.count("ref: ${{ github.workflow_sha }}"), 3)
        self.assertNotIn(
            "ref: ${{ steps.pr.outputs.base_sha || github.sha }}",
            workflow,
        )
        self.assertNotIn("ref: ${{ needs.detect.outputs.base_sha }}", workflow)
        self.assertNotIn(
            "ref: ${{ needs.detect.outputs.base_sha || needs.detect.outputs.checkout_sha }}",
            workflow,
        )
        # The stale base is still the correct boundary for the PR's three-dot diff.
        self.assertIn('"$PR_BASE_SHA...$PR_HEAD_SHA"', workflow)

    def test_relevant_pr_base_requires_a_successful_full_main_run(self) -> None:
        workflow = (
            GITHUB_DIR / "workflows" / "cpp_codecov.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("actions: read", workflow)
        self.assertIn("Classify PR base commit", workflow)
        self.assertIn('"${PR_BASE_SHA}^1"', workflow)
        self.assertIn(
            '"$base_parent" "$PR_BASE_SHA"',
            workflow,
        )
        self.assertIn(
            "BASE_CHANGE_RELEVANT: ${{ steps.base_change.outputs.should_run }}",
            workflow,
        )
        self.assertIn("github.rest.actions.listWorkflowRuns", workflow)
        self.assertIn("workflow_id: 'cpp_codecov.yml'", workflow)
        self.assertIn("head_sha: baseSha", workflow)
        self.assertIn("run.head_branch === 'main'", workflow)
        self.assertIn("run.event === 'push'", workflow)
        self.assertIn("run.status === 'completed' && run.conclusion === 'success'", workflow)
        self.assertIn(
            "process.env.BASE_CHANGE_RELEVANT === 'true' || runs.length > 0",
            workflow,
        )
        self.assertIn("Wait for or rerun the main coverage workflow", workflow)
        self.assertIn("then rerun this PR coverage workflow", workflow)

    def test_untrusted_yaml_is_loaded_safely(self) -> None:
        checks = (
            GITHUB_DIR / "workflows" / "cpp_codecov_checks.yml"
        ).read_text(encoding="utf-8")
        tests = Path(__file__).read_text(encoding="utf-8")
        self.assertIn(
            'YAML.safe_load(File.read(path), aliases: false)',
            checks,
        )
        self.assertIn(
            'YAML.safe_load(File.read(ARGV.fetch(0)), aliases: false)',
            tests,
        )
        unsafe_loader = "YAML." + "load_file"
        self.assertNotIn(unsafe_loader, checks)
        self.assertNotIn(unsafe_loader, tests)

    def test_codecov_upload_uses_explicit_safe_arguments(self) -> None:
        action = (
            GITHUB_DIR / "actions" / "run_clang_codecov" / "action.yaml"
        ).read_text(encoding="utf-8")
        self.assertIn('--slug "${REPOSITORY_SLUG}"', action)
        self.assertIn('--sha "${COMMIT_SHA}"', action)
        self.assertIn("github_token:", action)
        self.assertIn("Verify current PR head before publication", action)
        self.assertIn("if: ${{ inputs.pr_number != '' }}", action)
        self.assertIn("pr.state !== 'open'", action)
        self.assertIn("pr.head.sha !== expectedSha", action)
        self.assertIn("steps.pr_head.outcome == 'success'", action)
        self.assertLess(
            action.index("Verify current PR head before publication"),
            action.index("Upload to Codecov"),
        )
        self.assertNotIn("parent_sha", action)
        self.assertNotIn("PARENT_SHA", action)
        self.assertNotIn("--parent-sha", action)
        self.assertIn("--disable-search", action)
        self.assertIn("--plugin noop", action)
        self.assertIn('--html-input "$OUT/coverage.lcov"', action)
        self.assertIn('--html-output "$OUT/coverage.filtered.report"', action)
        self.assertIn('report_dir="${OUT}/coverage.filtered.report"', action)
        self.assertNotIn('report_dir="${OUT}/coverage.report"', action)
        self.assertIn("id: export", action)
        self.assertIn("id: html", action)
        self.assertGreaterEqual(action.count("steps.export.outcome == 'success'"), 3)
        self.assertEqual(action.count("steps.html.outcome == 'success'"), 2)
        self.assertIn('if [ ! -f "${report_dir}/index.html" ]', action)
        self.assertNotIn('-t "${CODECOV_TOKEN}"', action)
        self.assertNotIn("set -x", action)

    def test_landing_revalidates_pr_head_before_s3_upload(self) -> None:
        workflow = (
            GITHUB_DIR / "workflows" / "cpp_codecov.yml"
        ).read_text(encoding="utf-8")
        landing = workflow[workflow.index("  landing:") :]
        self.assertIn("Verify current PR head before landing publication", landing)
        self.assertIn("id: landing_pr_head", landing)
        self.assertIn(
            "EXPECTED_SHA: ${{ needs.detect.outputs.report_sha }}",
            landing,
        )
        self.assertIn(
            "PR_NUMBER: ${{ needs.detect.outputs.pr_number }}",
            landing,
        )
        self.assertIn("pr.state !== 'open' || pr.head.sha !== expectedSha", landing)
        self.assertIn("steps.landing_pr_head.outcome == 'success'", landing)
        self.assertLess(
            landing.index("Verify current PR head before landing publication"),
            landing.index("- name: Upload landing page"),
        )

    def test_pr_is_selective_and_main_always_runs_every_suite(self) -> None:
        workflow = (
            GITHUB_DIR / "workflows" / "cpp_codecov.yml"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "python3 .github/scripts/codecov/detect_codecov_matrix.py --all",
            workflow,
        )
        self.assertIn('if [ "$EVENT_NAME" = push ]', workflow)
        self.assertIn('"$PR_BASE_SHA...$PR_HEAD_SHA"', workflow)
        self.assertNotIn("resolve_codecov_parent", workflow)
        self.assertNotIn("parent_sha", workflow)
        self.assertNotIn("PARENT_SHA", workflow)
        self.assertNotIn("FORCE_FULL_CHECKPOINT", workflow)
        self.assertNotIn("--first-parent", workflow)
        self.assertNotIn("--diff-merges=first-parent", workflow)
        self.assertNotIn('".github/workflows/cpp_codecov_checks.yml"', workflow)
        self.assertIn("|| format('{0}-main', github.workflow)", workflow)
        self.assertEqual(workflow.count("concurrency:"), 1)
        self.assertIn("github_token: ${{ github.token }}", workflow)
        self.assertNotIn("suites=(cpp_sdk cli cli_workload)", workflow)


if __name__ == "__main__":
    unittest.main()
