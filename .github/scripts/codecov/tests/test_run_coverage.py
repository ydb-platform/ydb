#!/usr/bin/env python3

from __future__ import annotations

import copy
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest import mock

CODECOV_DIR = Path(__file__).resolve().parent.parent
GITHUB_DIR = CODECOV_DIR.parent.parent
sys.path.insert(0, str(CODECOV_DIR))

import run_coverage as coverage


def successful_report() -> dict:
    return {
        "results": [
            {"type": "test", "suite": True, "status": "OK", "path": "tests/example"},
            {"type": "test", "status": "OK", "name": "example"},
            {"type": "test", "status": "SKIPPED", "name": "optional"},
        ],
        "progress": {"linux": [{"type": "test", "done": 1, "total": 1, "size": "small"}]},
    }


def corruption_log(cwd: Path) -> str:
    return (
        f"command (pid: 123) /tools/test_tool resolve_clang_coverage "
        f"--coverage-path {cwd}/tests/example/test-results/coverage.tar "
        f"--log-path {cwd}/tests/example/test-results/clangcov_resolve.example.log "
        f"failed with exit code 255 in {cwd}\n"
        "warning: tmp/example.123.0clang.profraw: invalid instrumentation "
        "profile data (file header is corrupt)\n"
        "error: no profile can be merged\n"
    )


class RetryDecisionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.report = successful_report()
        self.details = coverage.evidence(corruption_log(Path("/attempt/build/node")))

    def test_retry_only_for_expected_failure_with_complete_successful_tests(self) -> None:
        self.assertTrue(coverage.tests_passed(self.report))
        self.assertTrue(coverage.should_retry(255, self.report, self.details))
        for code in (0, 1, 2, 130, 137, 143):
            with self.subTest(code=code):
                self.assertFalse(coverage.should_retry(code, self.report, self.details))

    def test_missing_failed_or_incomplete_report_disallows_retry(self) -> None:
        variants = [{}, {"results": []}, {"results": [{}]}]
        for status in ("FAIL", "FAILED", "TIMEOUT", "UNKNOWN", None):
            report = copy.deepcopy(self.report)
            report["results"][1]["status"] = status
            variants.append(report)
        for progress in ({}, None, {"linux": [{}]}, {"linux": {"done": 1}},
                         {"linux": [{"type": "build", "done": 1, "total": 1}]}):
            report = copy.deepcopy(self.report)
            report["progress"] = progress
            variants.append(report)
        for done, total in ((0, 1), (1, "1"), (-1, -1), (True, True), (0, 0)):
            report = copy.deepcopy(self.report)
            report["progress"]["linux"][0].update({"done": done, "total": total})
            variants.append(report)
        for result in ([self.report["results"][0]], [self.report["results"][1]],
                       [{"type": "test", "suite": True, "status": "SKIPPED"},
                        {"type": "test", "status": "SKIPPED"}]):
            report = copy.deepcopy(self.report)
            report["results"] = result
            variants.append(report)
        for report in variants:
            with self.subTest(report=report):
                self.assertFalse(coverage.should_retry(255, report, self.details))

    def test_skipped_suite_chunk_or_build_is_not_a_complete_success(self) -> None:
        for extra in ({"type": "test", "suite": True, "status": "SKIPPED"},
                      {"type": "test", "chunk": True, "status": "SKIPPED"},
                      {"type": "build", "status": "SKIPPED"}):
            report = copy.deepcopy(self.report)
            report["results"].append(extra)
            with self.subTest(extra=extra):
                self.assertFalse(coverage.should_retry(255, report, self.details))

    def test_unknown_failed_node_disallows_retry(self) -> None:
        log = corruption_log(Path("/attempt/build/node"))
        log += "command (pid: 124) /tools/clang++ input.cpp failed with exit code 1 in /attempt/build/compiler\n"
        details = coverage.evidence(log)
        self.assertTrue(details["unknown_failure"])
        self.assertFalse(coverage.should_retry(255, self.report, details))

    def test_corruption_message_and_coverage_node_are_both_required(self) -> None:
        for key in ("nodes", "bad_profiles"):
            details = copy.deepcopy(self.details)
            details[key] = []
            with self.subTest(key=key):
                self.assertFalse(coverage.should_retry(255, self.report, details))

    def test_ansi_output_is_parsed_and_profiles_are_deduplicated(self) -> None:
        log = corruption_log(Path("/attempt/build/node"))
        details = coverage.evidence("\x1b[31m" + log + log + "\x1b[0m")
        self.assertEqual(details["bad_profiles"], ["example.123.0clang.profraw"])
        self.assertFalse(details["unknown_failure"])

    def test_repeated_same_failed_command_still_allows_one_retry(self) -> None:
        log = corruption_log(Path("/attempt/build/node"))
        details = coverage.evidence(log + log)
        self.assertEqual(len(details["nodes"]), 1)
        coverage.merge_evidence(details, coverage.evidence(log))
        self.assertEqual(len(details["nodes"]), 1)
        self.assertTrue(coverage.should_retry(255, self.report, details))

    def test_two_distinct_failed_coverage_nodes_disallow_retry(self) -> None:
        first = corruption_log(Path("/attempt/build/first"))
        second = corruption_log(Path("/attempt/build/second"))
        details = coverage.evidence(first + second)
        self.assertEqual(len(details["nodes"]), 2)
        self.assertFalse(coverage.should_retry(255, self.report, details))

    def test_report_reader_rejects_invalid_or_missing_results(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "report.json"
            for content in ("not json", "[]", "{}", '{"results": {}}'):
                path.write_text(content, encoding="utf-8")
                with self.subTest(content=content), self.assertRaises(ValueError):
                    coverage.read_report(path)


class DiagnosticBundleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.allowed = self.root / "attempt"
        self.allowed.mkdir()
        self.redactor = coverage.Redactor({}, [])

    def bundle(self, **kwargs) -> coverage.Bundle:
        return coverage.Bundle(self.root / "bundle", self.redactor, **kwargs)

    def test_redacts_secret_environment_and_credential_json_values(self) -> None:
        credentials = self.root / "credentials.json"
        private_key = "-----BEGIN PRIVATE KEY-----\nprivate-line-value\n-----END PRIVATE KEY-----"
        credentials.write_text(json.dumps({"private_key": private_key, "id": "credential-id"}), encoding="utf-8")
        redact = coverage.Redactor({"BAZEL_REMOTE_PASSWORD": "cache-password", "NORMAL": "public-value"}, [credentials])
        text = "cache-password credential-id " + private_key + " " + json.dumps(private_key) + " public-value"
        result = redact(text)
        for secret in ("cache-password", "credential-id", "private-line-value"):
            self.assertNotIn(secret, result)
        self.assertIn("public-value", result)
        self.assertIn("[REDACTED]", result)

    def test_text_and_manifest_are_redacted_but_raw_bytes_are_unchanged(self) -> None:
        self.redactor = coverage.Redactor({"TEST_TOKEN": "secret-value"}, [])
        bundle = self.bundle()
        raw = self.allowed / "broken.profraw"
        raw.write_bytes(b"\x00secret-value\xff")
        log = self.allowed / "run_test.log"
        log.write_text("secret-value", encoding="utf-8")
        self.assertTrue(bundle.copy(raw, [self.allowed], raw=True))
        self.assertTrue(bundle.copy(log, [self.allowed]))
        bundle.finish({"description": "secret-value"})
        self.assertEqual(next(bundle.directory.glob("*.profraw")).read_bytes(), raw.read_bytes())
        self.assertNotIn("secret-value", next(bundle.directory.glob("*.log")).read_text())
        self.assertNotIn("secret-value", (bundle.directory / "manifest.json").read_text())

    def test_raw_profile_is_omitted_not_truncated_at_size_limit(self) -> None:
        raw = self.allowed / "broken.profraw"
        raw.write_bytes(b"x" * 33)
        bundle = self.bundle()
        with mock.patch.object(coverage, "PROFILE_LIMIT", 32):
            self.assertFalse(bundle.copy(raw, [self.allowed], raw=True))
        self.assertFalse(list(bundle.directory.glob("*.profraw")))
        self.assertTrue(any("size" in item.get("note", "") for item in bundle.entries))

    def test_logs_are_tail_truncated_and_bundle_budget_is_enforced(self) -> None:
        log = self.allowed / "run_test.log"
        log.write_bytes(b"older-output:" + b"tail" * 8)
        bundle = self.bundle(limit=coverage.MIB + 40)
        with mock.patch.object(coverage, "LOG_LIMIT", 32):
            bundle.copy(log, [self.allowed])
        saved = next(bundle.directory.glob("*.log"))
        self.assertEqual(saved.read_bytes(), b"tail" * 8)
        bundle.save("too-large.profraw", b"x" * 9, "raw-profile")
        self.assertFalse((bundle.directory / "too-large.profraw").exists())
        bundle.finish({})
        self.assertLessEqual(sum(path.stat().st_size for path in bundle.directory.iterdir()), coverage.MIB + 40)
        self.assertTrue(any("truncated" in item.get("note", "") for item in bundle.entries))

    def test_symlink_to_outside_and_missing_files_are_not_copied(self) -> None:
        outside = self.root / "credentials.json"
        outside.write_text("must-not-be-uploaded", encoding="utf-8")
        link = self.allowed / "run_test.log"
        link.symlink_to(outside)
        bundle = self.bundle()
        self.assertFalse(bundle.copy(link, [self.allowed]))
        self.assertFalse(bundle.copy(self.allowed / "missing.log", [self.allowed]))
        self.assertFalse(list(bundle.directory.iterdir()))
        self.assertTrue(any("outside" in item.get("note", "") for item in bundle.entries))
        self.assertTrue(any("unavailable" in item.get("note", "") for item in bundle.entries))

    def test_tar_profiles_use_safe_output_names_and_ignore_links_and_unrelated_files(self) -> None:
        archive = self.allowed / "coverage.tar"
        with tarfile.open(archive, "w") as tar:
            for name, data in (("../../broken.profraw", b"complete-raw-profile"),
                               ("credentials.json", b"must-not-be-uploaded")):
                info = tarfile.TarInfo(name)
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
            link = tarfile.TarInfo("linked.profraw")
            link.type = tarfile.SYMTYPE
            link.linkname = "../../credentials.json"
            tar.addfile(link)
        bundle = self.bundle()
        wanted = {"broken.profraw", "linked.profraw"}
        with mock.patch.object(tarfile.TarFile, "extractall", side_effect=AssertionError("unsafe extraction")):
            bundle.profile_from_tar(archive, wanted, [self.allowed])
        self.assertEqual(wanted, {"linked.profraw"})
        self.assertEqual(next(bundle.directory.glob("*.profraw")).read_bytes(), b"complete-raw-profile")
        self.assertFalse((self.root / "broken.profraw").exists())
        self.assertEqual(len(list(bundle.directory.iterdir())), 1)

    def test_oversized_tar_profile_is_omitted_whole(self) -> None:
        archive = self.allowed / "coverage.tar"
        with tarfile.open(archive, "w") as tar:
            info = tarfile.TarInfo("broken.profraw")
            info.size = 33
            tar.addfile(info, io.BytesIO(b"x" * 33))
        bundle = self.bundle()
        wanted = {"broken.profraw"}
        with mock.patch.object(coverage, "PROFILE_LIMIT", 32):
            bundle.profile_from_tar(archive, wanted, [self.allowed])
        self.assertEqual(wanted, {"broken.profraw"})
        self.assertFalse(list(bundle.directory.iterdir()))

    def test_missing_and_invalid_archives_do_not_abort_collection(self) -> None:
        bundle = self.bundle()
        bundle.profile_from_tar(self.allowed / "missing.tar", {"broken.profraw"}, [self.allowed])
        archive = self.allowed / "coverage.tar"
        archive.write_bytes(b"not a tar file")
        bundle.profile_from_tar(archive, {"broken.profraw"}, [self.allowed])
        self.assertFalse(list(bundle.directory.iterdir()))
        self.assertTrue(any("could not inspect" in item.get("note", "") for item in bundle.entries))

    def test_archive_symlink_outside_attempt_is_not_read(self) -> None:
        archive = self.root / "outside.tar"
        with tarfile.open(archive, "w") as tar:
            info = tarfile.TarInfo("broken.profraw")
            info.size = 6
            tar.addfile(info, io.BytesIO(b"secret"))
        link = self.allowed / "coverage.tar"
        link.symlink_to(archive)
        bundle = self.bundle()
        bundle.profile_from_tar(link, {"broken.profraw"}, [self.allowed])
        self.assertFalse(list(bundle.directory.iterdir()))


class CoverageCollectorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.build_root = self.root / "attempt/build"
        self.node = self.build_root / "report-node"
        self.node.mkdir(parents=True)
        self.output = self.root / "attempt/output"
        self.output.mkdir()
        self.archives = [
            self.node / "tests/first/test-results/unittest/coverage.tar",
            self.node / "tests/second/test-results/unittest/coverage.tar",
        ]
        self.profile_name = "example.123.0clang.profraw"
        self.log = self.root / "attempt/ya.log"

    def report_failure_log(self, profile_path: str, work_dir: Path | None = None) -> str:
        archives = " ".join(f"--coverage-path {path}" for path in self.archives)
        return (
            f"command (pid: 123) /tools/test_tool build_clang_coverage_report {archives} "
            f"failed with exit code 255 in {self.node}\n"
            f"warning: {profile_path}: invalid instrumentation profile data (file header is corrupt)\n"
            + (f"Tests run working directory: {work_dir}\n" if work_dir else "")
        )

    def write_archive(self, archive: Path, members: dict[str, bytes]) -> None:
        archive.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(archive, "w") as tar:
            for name, data in members.items():
                member = tarfile.TarInfo(name)
                member.size = len(data)
                tar.addfile(member, io.BytesIO(data))

    def collect(self, text: str, name: str = "bundle") -> tuple[Path, dict]:
        self.log.write_text(text, encoding="utf-8")
        directory = self.root / name
        coverage.collect_diagnostics(
            directory, self.build_root, self.output, self.log,
            successful_report(), coverage.evidence(text), coverage.Redactor({}, []), {"attempt": 1},
        )
        return directory, json.loads((directory / "manifest.json").read_text())

    def test_nested_raw_profile_and_all_report_archive_paths_are_preserved(self) -> None:
        relative = f"tmp/0/{self.profile_name}"
        raw = self.node / relative
        raw.parent.mkdir(parents=True)
        raw.write_bytes(b"complete-indexed-raw\x00\xff")
        directory, manifest = self.collect(self.report_failure_log(relative))
        details = manifest["evidence"]
        self.assertEqual(details["profile_paths"], [relative])
        self.assertEqual(details["nodes"][0]["kind"], "build_clang_coverage_report")
        self.assertEqual(details["nodes"][0]["coverage_paths"], [str(path) for path in self.archives])
        self.assertEqual(next(directory.glob("*.profraw")).read_bytes(), raw.read_bytes())
        entry = next(item for item in manifest["files"] if item.get("source") == str(raw) and "file" in item)
        self.assertEqual(entry["source_bytes"], raw.stat().st_size)
        self.assertEqual(entry["source_mtime_ns"], raw.stat().st_mtime_ns)

    def test_missing_nested_profile_is_recovered_from_first_archive_not_only_last(self) -> None:
        self.write_archive(self.archives[0], {self.profile_name: b"first-archive-raw"})
        self.write_archive(self.archives[1], {"unrelated.profraw": b"unrelated"})
        directory, manifest = self.collect(self.report_failure_log(f"tmp/0/{self.profile_name}"))
        self.assertEqual(next(directory.glob("*.profraw")).read_bytes(), b"first-archive-raw")
        self.assertTrue(any(item.get("source") == f"{self.archives[0]}:{self.profile_name}" for item in manifest["files"]))

    def test_fallback_uses_indexed_archive_when_profile_basenames_collide(self) -> None:
        self.write_archive(self.archives[0], {self.profile_name: b"healthy-profile-from-other-suite"})
        self.write_archive(self.archives[1], {self.profile_name: b"damaged-profile-from-second-suite"})
        directory, manifest = self.collect(self.report_failure_log(f"tmp/1/{self.profile_name}"))
        self.assertEqual(next(directory.glob("*.profraw")).read_bytes(), b"damaged-profile-from-second-suite")
        self.assertTrue(any(item.get("source") == f"{self.archives[1]}:{self.profile_name}" for item in manifest["files"]))

    def test_printed_profile_paths_cannot_collect_outside_node_or_attempt(self) -> None:
        outside = self.root / self.profile_name
        outside.write_bytes(b"outside-secret-data")
        sibling = self.build_root / self.profile_name
        sibling.write_bytes(b"other-node-private-data")
        linked = self.node / f"tmp/0/{self.profile_name}"
        linked.parent.mkdir(parents=True)
        linked.symlink_to(outside)
        cases = [str(outside), os.path.relpath(outside, self.node), str(sibling), f"tmp/0/{self.profile_name}"]
        for index, profile_path in enumerate(cases):
            with self.subTest(path=profile_path):
                directory, manifest = self.collect(self.report_failure_log(profile_path), f"outside-{index}")
                self.assertFalse(list(directory.glob("*.profraw")))
                self.assertTrue(any("outside" in item.get("note", "") for item in manifest["files"]))
        outside_archive = self.root / "outside-coverage.tar"
        self.write_archive(outside_archive, {self.profile_name: b"outside-archive-private-data"})
        self.archives[0] = outside_archive
        directory, _ = self.collect(self.report_failure_log(f"tmp/0/missing/{self.profile_name}"), "outside-archive")
        self.assertFalse(list(directory.glob("*.profraw")))

    def test_unsplit_work_directory_recovers_matching_suite_logs(self) -> None:
        work_dir = self.build_root / "test-node/tests/first/test-results/unittest"
        work_dir.mkdir(parents=True)
        log = work_dir / "run_test.log"
        log.write_text("unsplit-suite-log", encoding="utf-8")
        directory, manifest = self.collect(self.report_failure_log(f"tmp/0/{self.profile_name}", work_dir))
        self.assertEqual(next(directory.glob("*-run_test.log")).read_text(), "unsplit-suite-log")
        self.assertTrue(any(item.get("source") == str(log) for item in manifest["files"]))

    def test_multiple_failed_nodes_keep_logs_but_omit_ambiguous_raw_profiles(self) -> None:
        nodes = [self.node, self.build_root / "second-node"]
        for index, node in enumerate(nodes):
            raw = node / "tmp" / self.profile_name
            raw.parent.mkdir(parents=True)
            raw.write_bytes(f"raw-from-node-{index}".encode())
            log = node / "tests/example/test-results/clangcov_resolve.example.log"
            log.parent.mkdir(parents=True)
            log.write_text(f"resolver-log-{index}", encoding="utf-8")
        directory, manifest = self.collect("".join(corruption_log(node) for node in nodes))
        self.assertFalse(list(directory.glob("*.profraw")))
        logs = sorted(path.read_text() for path in directory.glob("*-clangcov_resolve.example.log"))
        self.assertEqual(logs, ["resolver-log-0", "resolver-log-1"])
        self.assertTrue((directory / "test-results.json").is_file())
        self.assertTrue(any(
            item.get("note") == "multiple failed nodes: raw profiles omitted and automatic retry disabled"
            for item in manifest["files"]
        ))


# A stand-in for ya: it is launched as a real child process by the production
# wrapper, writes the requested report/artifacts, and never builds project code.
FAKE_YA = r'''
import json
import os
from pathlib import Path
import signal
import sys
import time

args = sys.argv[1:]
def value(flag):
    return Path(args[args.index(flag) + 1])

output = value("--output")
build_root = value("--build-root")
report_path = value("--build-results-report")
retest = "--retest" in args
with open(os.environ["COVERAGE_TEST_CALLS"], "a") as stream:
    stream.write(json.dumps({"args": args, "output": str(output), "build_root": str(build_root)}) + "\n")
scenario = os.environ["COVERAGE_TEST_SCENARIO"]
if scenario == "success_first" or (retest and scenario == "retry_success"):
    (output / "successful-output").write_text("success")
    sys.exit(0)

report = {
    "results": [
        {"type": "test", "suite": True, "status": "OK", "path": "tests/example"},
        {"type": "test", "status": "OK", "name": "example"},
    ],
    "progress": {"linux": [{"type": "test", "done": 1, "total": 1, "size": "small"}]},
}
if scenario == "failed_tests":
    report["results"][1]["status"] = "FAIL"
if scenario == "incomplete_report":
    report["progress"]["linux"][0]["done"] = 0
if scenario != "missing_report":
    report_path.write_text(json.dumps(report))

node = build_root / "node"
(node / "tmp").mkdir(parents=True)
suite = Path("tests/example/test-results")
(node / suite).mkdir(parents=True)
log = node / suite / "clangcov_resolve.example.log"
log.write_text("resolve failure " + os.environ.get("COVERAGE_TEST_TOKEN", ""))
if scenario != "missing_artifacts":
    (node / "tmp/example.123.0clang.profraw").write_bytes(b"complete-broken-profile")
(output / suite).mkdir(parents=True)
(output / suite / "run_test.log").write_text("suite-log " + os.environ.get("COVERAGE_TEST_TOKEN", ""))
(output / suite / "credentials.json").write_text("must-not-be-uploaded")
if scenario in ("unknown_failure", "unknown_before_tail"):
    print(f"command (pid: 124) /tools/compiler input.cpp failed with exit code 1 in {node}")
if scenario == "unknown_before_tail":
    for _ in range(9 * 1024):
        print("x" * 1024)
print(f"command (pid: 123) /tools/test_tool resolve_clang_coverage --coverage-path {node / suite}/coverage.tar --log-path {log} failed with exit code 255 in {node}")
print("warning: tmp/example.123.0clang.profraw: invalid instrumentation profile data (file header is corrupt)")
print("error: no profile can be merged", flush=True)
if scenario == "cancelled":
    # Even a process which exits 255 after cancellation must not trigger retry.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(255))
    os.kill(os.getppid(), signal.SIGTERM)
    time.sleep(0.5)
sys.exit(255)
'''


class CoverageSubprocessTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def run_scenario(self, scenario: str) -> tuple[subprocess.CompletedProcess, list[dict], dict[str, str]]:
        calls = self.root / "calls.jsonl"
        github_output = self.root / "github-output"
        env = dict(os.environ)
        env.pop("CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS", None)
        env.update({
            "COVERAGE_TEST_SCENARIO": scenario,
            "COVERAGE_TEST_CALLS": str(calls),
            "COVERAGE_TEST_TOKEN": "secret-sentinel-value",
            "GITHUB_OUTPUT": str(github_output),
        })
        completed = subprocess.run(
            [sys.executable, str(CODECOV_DIR / "run_coverage.py"), "--suite", "cli",
             "--work-dir", str(self.root / "work"), "--", sys.executable, "-c", FAKE_YA],
            capture_output=True, text=True, env=env, timeout=30,
        )
        invocations = [json.loads(line) for line in calls.read_text().splitlines()] if calls.exists() else []
        outputs = dict(line.split("=", 1) for line in github_output.read_text().splitlines()) if github_output.exists() else {}
        return completed, invocations, outputs

    def test_successful_first_attempt_has_no_retry_or_failure_bundle(self) -> None:
        result, calls, outputs = self.run_scenario("success_first")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 1)
        self.assertNotIn("--retest", calls[0]["args"])
        self.assertEqual(outputs["out_dir"], calls[0]["output"])
        self.assertNotIn("had_failure", outputs)
        self.assertFalse(Path(outputs["diagnostics_dir"]).exists())

    def test_retry_uses_fresh_roots_and_preserves_first_failure_diagnostics(self) -> None:
        result, calls, outputs = self.run_scenario("retry_success")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 2)
        self.assertNotIn("--retest", calls[0]["args"])
        self.assertIn("--retest", calls[1]["args"])
        for key in ("output", "build_root"):
            self.assertNotEqual(calls[0][key], calls[1][key])
            self.assertTrue(Path(calls[0][key]).is_dir())
        self.assertIn("/.ya/build/build_root", calls[1]["build_root"])
        self.assertEqual(outputs["out_dir"], calls[1]["output"])
        self.assertEqual(outputs["had_failure"], "true")
        diagnostics = Path(outputs["diagnostics_dir"])
        first = diagnostics / "attempt-1"
        self.assertTrue((first / "manifest.json").is_file())
        self.assertFalse((diagnostics / "attempt-2").exists())
        self.assertEqual(next(first.glob("*.profraw")).read_bytes(), b"complete-broken-profile")
        combined_text = "\n".join(path.read_text() for path in first.iterdir() if path.suffix != ".profraw")
        self.assertIn("suite-log", combined_text)
        self.assertNotIn("secret-sentinel-value", combined_text)
        self.assertNotIn("must-not-be-uploaded", combined_text)
        manifest = json.loads((first / "manifest.json").read_text())
        self.assertTrue(manifest["retry"])

    def test_second_failure_stays_failed_and_never_runs_a_third_attempt(self) -> None:
        result, calls, outputs = self.run_scenario("repeated_error")
        self.assertEqual(result.returncode, 255, result.stderr)
        self.assertEqual(len(calls), 2)
        self.assertNotIn("out_dir", outputs)
        second = Path(outputs["diagnostics_dir"]) / "attempt-2/manifest.json"
        self.assertFalse(json.loads(second.read_text())["retry"])

    def test_failed_tests_missing_report_or_incomplete_progress_prevent_retry(self) -> None:
        for scenario in ("failed_tests", "missing_report", "incomplete_report", "unknown_failure"):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as temporary:
                self.root = Path(temporary)
                result, calls, outputs = self.run_scenario(scenario)
                self.assertEqual(result.returncode, 255, result.stderr)
                self.assertEqual(len(calls), 1)
                self.assertNotIn("out_dir", outputs)

    def test_failure_outside_saved_log_tail_still_prevents_retry(self) -> None:
        result, calls, outputs = self.run_scenario("unknown_before_tail")
        self.assertEqual(result.returncode, 255, result.stderr)
        self.assertEqual(len(calls), 1)
        self.assertNotIn("out_dir", outputs)

    def test_cancelled_attempt_never_retries_even_when_child_returns_255(self) -> None:
        result, calls, outputs = self.run_scenario("cancelled")
        self.assertNotEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 1)
        self.assertNotIn("out_dir", outputs)
        manifest = Path(outputs["diagnostics_dir"]) / "attempt-1/manifest.json"
        self.assertTrue(json.loads(manifest.read_text())["cancelled"])

    def test_missing_diagnostic_profiles_do_not_hide_failure_or_skip_second_attempt(self) -> None:
        result, calls, outputs = self.run_scenario("missing_artifacts")
        self.assertEqual(result.returncode, 255, result.stderr)
        self.assertEqual(len(calls), 2)
        first = Path(outputs["diagnostics_dir"]) / "attempt-1/manifest.json"
        manifest = json.loads(first.read_text())
        self.assertTrue(any("not recovered" in item.get("note", "") for item in manifest["files"]))
        self.assertNotIn("out_dir", outputs)


class CoverageWorkflowContractTest(unittest.TestCase):
    def test_action_delegates_output_publication_and_retry_to_wrapper(self) -> None:
        action = (GITHUB_DIR / "actions/run_clang_codecov/action.yaml").read_text()
        ya_step = action.split("    - name: Ya make with clang coverage", 1)[1].split("    - name:", 1)[0]
        self.assertIn("python3 .github/scripts/codecov/run_coverage.py", ya_step)
        self.assertIn('--secret-file "$BAZEL_REMOTE_PASSWORD_FILE"', ya_step)
        self.assertIn('-- ./ya make "${params[@]}" "${TARGETS[@]}"', ya_step)
        self.assertNotIn("out_dir=", ya_step)
        self.assertNotIn("--output", ya_step)
        self.assertNotIn("continue-on-error:", ya_step)

    def test_failure_artifacts_survive_successful_retry_and_have_bounded_retention(self) -> None:
        action = (GITHUB_DIR / "actions/run_clang_codecov/action.yaml").read_text()
        artifact = action.split("    - name: Upload coverage failure diagnostics", 1)[1].split("    - name:", 1)[0]
        self.assertIn("always() && steps.ya.outputs.had_failure == 'true'", artifact)
        self.assertIn("continue-on-error: true", artifact)
        self.assertIn("uses: actions/upload-artifact@v4", artifact)
        self.assertIn("retention-days: 7", artifact)
        self.assertIn("path: ${{ steps.ya.outputs.diagnostics_dir }}", artifact)
        self.assertIn("if-no-files-found: warn", artifact)
        name = re.search(r"^\s+name: (.+)$", artifact, re.M).group(1)
        for value in ("inputs.suite", "github.run_id", "github.run_attempt"):
            self.assertIn("${{ " + value + " }}", name)

    def test_helper_workflow_discovers_the_new_tests(self) -> None:
        workflow = (GITHUB_DIR / "workflows/cpp_codecov_checks.yml").read_text()
        self.assertIn("python3 -m unittest discover -s .github/scripts/codecov/tests -p 'test_*.py' -v", workflow)
        self.assertIn(".github/scripts/codecov/run_coverage.py", workflow)
        self.assertIn(".github/scripts/codecov/tests/test_run_coverage.py", workflow)


if __name__ == "__main__":
    unittest.main()
