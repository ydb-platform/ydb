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
from resolve_codecov_parent import (
    CodecovApi,
    FULL_CHECKPOINT_INTERVAL,
    ResolverError,
    _expected_actual_flags,
    _resolve_parent,
    resolve_parent_sha,
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


class ResolveCodecovParentTest(unittest.TestCase):
    REQUIRED_FLAGS = {"cpp_sdk", "cli", "cli_workload"}
    HEAD = "f" * 40
    MAIN_ANCHOR = "e" * 40
    RECENT = "d" * 40
    FULL = "c" * 40

    class FakeApi:
        def __init__(
            self,
            commits: list[dict],
            flags: dict[str, set[str]],
            actual_flags: dict[str, set[str]] | None = None,
        ) -> None:
            self.commits = commits
            self.flags = flags
            self.actual_flags = flags if actual_flags is None else actual_flags
            self.requested_flags: list[str] = []

        def branch_commits(self, branch: str) -> list[dict]:
            if branch != "main":
                raise AssertionError(f"unexpected branch: {branch}")
            return self.commits

        def commit_flag_sets(self, sha: str) -> tuple[set[str], set[str]]:
            self.requested_flags.append(sha)
            return self.flags[sha], self.actual_flags[sha]

    @staticmethod
    def commit(sha: str, sessions: int = 1) -> dict:
        return {
            "commitid": sha,
            "state": "complete",
            "totals": {"sessions": sessions},
        }

    def resolution(
        self,
        api: FakeApi,
        ancestors: set[str] | None = None,
        expected_flags: dict[tuple[str, str], set[str]] | None = None,
        history_shas: list[str] | None = None,
    ) -> tuple[str, bool]:
        history_shas = history_shas or [self.MAIN_ANCHOR, self.RECENT, self.FULL]
        history = "\n".join(history_shas) + "\n"
        git_result = subprocess.CompletedProcess(
            args=["git", "rev-list"],
            returncode=0,
            stdout=history,
            stderr="",
        )
        valid_ancestors = ancestors if ancestors is not None else set(history_shas[1:])
        with mock.patch("resolve_codecov_parent._require_local_commit"), mock.patch(
            "resolve_codecov_parent._run_git", return_value=git_result
        ), mock.patch(
            "resolve_codecov_parent._is_ancestor",
            side_effect=lambda candidate, _head: candidate in valid_ancestors,
        ), mock.patch(
            "resolve_codecov_parent._expected_actual_flags",
            side_effect=lambda baseline, commit, _required: (
                expected_flags or {}
            ).get((baseline, commit), set()),
        ):
            return _resolve_parent(
                api,
                "main",
                self.MAIN_ANCHOR,
                self.HEAD,
                self.REQUIRED_FLAGS,
            )

    def resolve(
        self,
        api: FakeApi,
        ancestors: set[str] | None = None,
        expected_flags: dict[tuple[str, str], set[str]] | None = None,
    ) -> str:
        return self.resolution(api, ancestors, expected_flags)[0]

    def test_nearest_complete_ancestor_is_selected(self) -> None:
        api = self.FakeApi(
            [self.commit(self.RECENT), self.commit(self.FULL)],
            {self.RECENT: self.REQUIRED_FLAGS, self.FULL: self.REQUIRED_FLAGS},
        )
        self.assertEqual(self.resolve(api), self.RECENT)
        self.assertEqual(api.requested_flags, [self.RECENT])

    def test_incomplete_report_is_skipped(self) -> None:
        api = self.FakeApi(
            [self.commit(self.RECENT), self.commit(self.FULL)],
            {
                self.RECENT: {"cli", "cpp_sdk"},
                self.FULL: self.REQUIRED_FLAGS,
            },
        )
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(self.resolve(api), self.FULL)
        self.assertEqual(api.requested_flags, [self.RECENT, self.FULL])

    def test_main_report_ahead_of_stale_pr_head_is_skipped(self) -> None:
        api = self.FakeApi(
            [self.commit(self.MAIN_ANCHOR), self.commit(self.FULL)],
            {self.MAIN_ANCHOR: self.REQUIRED_FLAGS, self.FULL: self.REQUIRED_FLAGS},
        )
        self.assertEqual(self.resolve(api, ancestors={self.FULL}), self.FULL)
        self.assertEqual(api.requested_flags, [self.FULL])

    def test_carried_flag_without_a_fresh_upload_is_skipped(self) -> None:
        api = self.FakeApi(
            [self.commit(self.RECENT), self.commit(self.FULL)],
            {self.RECENT: self.REQUIRED_FLAGS, self.FULL: self.REQUIRED_FLAGS},
            actual_flags={self.RECENT: {"cli"}, self.FULL: self.REQUIRED_FLAGS},
        )
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(
                self.resolve(
                    api,
                    expected_flags={(self.FULL, self.RECENT): {"cli", "cpp_sdk"}},
                ),
                self.FULL,
            )
        self.assertEqual(api.requested_flags, [self.RECENT, self.FULL])

    def test_net_zero_report_with_a_fresh_upload_is_trusted(self) -> None:
        api = self.FakeApi(
            [self.commit(self.RECENT), self.commit(self.FULL)],
            {self.RECENT: self.REQUIRED_FLAGS, self.FULL: self.REQUIRED_FLAGS},
            actual_flags={self.RECENT: {"cli"}, self.FULL: self.REQUIRED_FLAGS},
        )
        self.assertEqual(
            self.resolve(
                api,
                expected_flags={(self.FULL, self.RECENT): {"cli"}},
            ),
            self.RECENT,
        )

    def test_carried_only_report_is_not_trusted(self) -> None:
        api = self.FakeApi(
            [self.commit(self.RECENT), self.commit(self.FULL)],
            {self.RECENT: self.REQUIRED_FLAGS, self.FULL: self.REQUIRED_FLAGS},
            actual_flags={self.RECENT: set(), self.FULL: self.REQUIRED_FLAGS},
        )
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(
                self.resolve(
                    api,
                    expected_flags={(self.FULL, self.RECENT): {"cli"}},
                ),
                self.FULL,
            )

    def test_expected_actual_flags_follow_changes_since_parent(self) -> None:
        parent = "a" * 40
        commit = "b" * 40
        changed = subprocess.CompletedProcess(
            args=["git", "diff"],
            returncode=0,
            stdout=(
                "ydb/apps/ydb/commands.cpp\n"
                "ydb/public/sdk/cpp/client.cpp\n"
            ),
            stderr="",
        )
        with mock.patch("resolve_codecov_parent._require_local_commit"), mock.patch(
            "resolve_codecov_parent._is_ancestor", return_value=True
        ), mock.patch("resolve_codecov_parent._run_git", return_value=changed):
            self.assertEqual(
                _expected_actual_flags(parent, commit, self.REQUIRED_FLAGS),
                {"cli", "cpp_sdk"},
            )

    @staticmethod
    def _git(repository: Path, *args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(repository), *args],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ).stdout.strip()

    def _commit(self, repository: Path, message: str) -> str:
        self._git(repository, "add", "-A")
        self._git(
            repository,
            "-c",
            "user.name=Codecov Test",
            "-c",
            "user.email=codecov-test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            message,
        )
        return self._git(repository, "rev-parse", "HEAD")

    def _push_suites(
        self,
        repository: Path,
        baseline: str,
        event_after: str,
    ) -> list[str]:
        changed: set[str] = set()
        output = self._git(
            repository,
            "-c",
            "core.quotePath=false",
            "log",
            "--first-parent",
            "--diff-merges=first-parent",
            "--format=",
            "--name-only",
            "--no-renames",
            f"{baseline}..{event_after}",
            "--",
        )
        changed.update(output.splitlines())
        return suites_from_paths(sorted(changed))

    def test_push_union_recovers_a_suite_changed_before_the_current_push(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repository = Path(temp_dir)
            self._git(repository, "init", "--quiet")
            (repository / "README.md").write_text("baseline\n", encoding="utf-8")
            baseline = self._commit(repository, "baseline")

            sdk = repository / "ydb/public/sdk/cpp/client.cpp"
            sdk.parent.mkdir(parents=True)
            sdk.write_text("sdk\n", encoding="utf-8")
            self._commit(repository, "sdk change")

            cli = repository / "ydb/apps/ydb/command.cpp"
            cli.parent.mkdir(parents=True)
            cli.write_text("cli\n", encoding="utf-8")
            current = self._commit(repository, "cli change")

            self.assertEqual(
                self._push_suites(repository, baseline, current),
                ["cli", "cpp_sdk"],
            )

    def test_push_union_keeps_a_revert_to_the_baseline_visible(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repository = Path(temp_dir)
            self._git(repository, "init", "--quiet")
            (repository / "README.md").write_text("baseline\n", encoding="utf-8")
            baseline = self._commit(repository, "baseline")

            sdk = repository / "ydb/public/sdk/cpp/client.cpp"
            sdk.parent.mkdir(parents=True)
            sdk.write_text("sdk\n", encoding="utf-8")
            self._commit(repository, "sdk change")

            sdk.unlink()
            current = self._commit(repository, "revert sdk change")
            self.assertEqual(self._git(repository, "diff", "--name-only", baseline, current), "")
            self.assertEqual(
                self._push_suites(repository, baseline, current),
                ["cpp_sdk"],
            )

    def test_missing_complete_ancestor_fails_closed(self) -> None:
        api = self.FakeApi(
            [self.commit(self.RECENT)],
            {self.RECENT: {"cli"}},
        )
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaisesRegex(ResolverError, "No complete Codecov main baseline"):
                self.resolve(api)

    def test_invalid_sha_is_rejected_before_api_access(self) -> None:
        api = self.FakeApi([], {})
        with self.assertRaisesRegex(ResolverError, "main anchor SHA"):
            resolve_parent_sha(api, "main", "not-a-sha", self.HEAD, self.REQUIRED_FLAGS)

    def test_pagination_uses_next_without_requiring_total_pages(self) -> None:
        api = CodecovApi("ydb-platform/ydb")
        responses = [
            {"results": [{"id": 1}], "next": "https://example.invalid/untrusted?page=2"},
            {"results": [{"id": 2}], "next": None},
        ]
        with mock.patch.object(api, "_request", side_effect=responses) as request:
            self.assertEqual(api._paginated_results("/fixed/path", {"branch": "main"}), [
                {"id": 1},
                {"id": 2},
            ])
        self.assertEqual(
            request.call_args_list,
            [
                mock.call("/fixed/path", {"branch": "main", "page": 1, "page_size": 100}),
                mock.call("/fixed/path", {"branch": "main", "page": 2, "page_size": 100}),
            ],
        )

    def test_full_checkpoint_threshold_is_bounded(self) -> None:
        for report_count, expected_force_full in (
            (FULL_CHECKPOINT_INTERVAL - 1, False),
            (FULL_CHECKPOINT_INTERVAL, True),
        ):
            with self.subTest(report_count=report_count):
                seed = "1" * 40
                incremental = [f"{value:040x}" for value in range(2, 2 + report_count)]
                history = [self.MAIN_ANCHOR, *reversed(incremental), seed]
                flags = {seed: self.REQUIRED_FLAGS}
                actual_flags = {seed: self.REQUIRED_FLAGS}
                for sha in incremental:
                    flags[sha] = self.REQUIRED_FLAGS
                    actual_flags[sha] = {"cli"}
                api = self.FakeApi(
                    [self.commit(sha) for sha in reversed(history[1:])],
                    flags,
                    actual_flags,
                )
                expected = {
                    (seed if index == 0 else incremental[index - 1], sha): {"cli"}
                    for index, sha in enumerate(incremental)
                }
                with contextlib.redirect_stderr(io.StringIO()):
                    parent, force_full = self.resolution(
                        api,
                        expected_flags=expected,
                        history_shas=history,
                    )
                self.assertEqual(parent, incremental[-1])
                self.assertIs(force_full, expected_force_full)


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
        self.assertIs(config["comment"]["hide_project_coverage"], True)
        self.assertIs(config["comment"]["show_carryforward_flags"], False)
        self.assertEqual(
            config["coverage"]["status"]["default_rules"]
            ["flag_coverage_not_uploaded_behavior"],
            "exclude",
        )
        for status in config["coverage"]["status"]["patch"].values():
            if isinstance(status, dict):
                self.assertIs(status["informational"], True)
        flags = {
            item["name"]: item["paths"]
            for item in config["flag_management"]["individual_flags"]
        }
        expected = {
            name: [f"{prefix}**" for prefix in cfg["lcov_prefixes"]]
            for name, cfg in SUITES.items()
        }
        self.assertEqual(flags, expected)

        cli_roots = tuple(SUITES["cli"]["lcov_prefixes"])
        for component in config["component_management"]["individual_components"]:
            for component_path in component["paths"]:
                self.assertTrue(component_path.startswith(cli_roots))

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
        self.assertIn('--parent-sha "${PARENT_SHA}"', action)
        self.assertIn('PARENT_SHA: ${{ inputs.parent_sha }}', action)
        self.assertIn('if [[ ! "${PARENT_SHA}" =~ ^[0-9a-fA-F]{40}$ ]]', action)
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

    def test_complete_codecov_parent_is_resolved_once_and_forwarded(self) -> None:
        workflow = (
            GITHUB_DIR / "workflows" / "cpp_codecov.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("parent_sha: ${{ steps.parent.outputs.parent_sha }}", workflow)
        self.assertIn(".github/scripts/codecov/resolve_codecov_parent.py", workflow)
        self.assertIn('main_anchor="$PR_BASE_SHA"', workflow)
        self.assertIn('report_head="$PR_HEAD_SHA"', workflow)
        self.assertIn('main_anchor=$(git rev-parse --verify "${EVENT_SHA}^1")', workflow)
        self.assertIn("--output-format github", workflow)
        self.assertIn('FORCE_FULL_CHECKPOINT: ${{ steps.parent.outputs.force_full }}', workflow)
        self.assertIn('if [ "$FORCE_FULL_CHECKPOINT" = true ]', workflow)
        self.assertIn("parent_sha: ${{ needs.detect.outputs.parent_sha }}", workflow)
        self.assertIn('"$CODECOV_PARENT_SHA..$AFTER_SHA"', workflow)
        self.assertIn("--first-parent", workflow)
        self.assertIn("--diff-merges=first-parent", workflow)
        self.assertIn("| sed '/^$/d' | sort -u", workflow)
        self.assertLess(
            workflow.index("- name: Resolve complete Codecov parent"),
            workflow.index("- name: Collect changed files"),
        )


if __name__ == "__main__":
    unittest.main()
