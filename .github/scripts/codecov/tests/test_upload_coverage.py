#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import upload_coverage


class UploadCoverageTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="codecov-upload-tests-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name).resolve()
        self.root = self.directory / "checkout"
        self.root.mkdir()
        self.run_git(self.root, "init", "-q")
        self.sources = {
            "ydb/apps/ydb/main.cpp": b"int main() {\n    return 0;\n}\n",
            "ydb/public/lib/ydb_cli/common.h": b"// header\n\nint helper();\n",
        }
        for name, content in {
            **self.sources,
            "ydb/public/sdk/cpp/client.cpp": b"int sdk();\n",
            "ydb/library/backup/backup.cpp": b"int backup();\n",
            "unrelated/huge.cpp": b"int unrelated();\n" * 100,
            ".agents/private.txt": b"not part of coverage\n",
            ".github/codecov.yml": b"coverage:\n  status:\n    project: off\n",
        }.items():
            self.write(name, content)
        self.run_git(self.root, "add", "--all")
        self.report = self.directory / "coverage.lcov"
        self.write_report(list(self.sources))
        self.capture = self.directory / "capture.json"
        self.summary = self.directory / "summary.md"
        self.temp_dir = self.directory / "temporary"
        self.temp_dir.mkdir()
        self.cli = self.directory / "fake-codecov"
        self.cli.write_text(
            f"#!{sys.executable}\n"
            "import json, os, pathlib, subprocess, sys, time\n"
            "root = pathlib.Path.cwd()\n"
            "files = {str(p.relative_to(root)): p.read_bytes().hex() "
            "for p in root.rglob('*') if p.is_file()}\n"
            "data = {'cwd': str(root), 'args': sys.argv[1:], 'files': files, "
            "'git_env': {k: v for k, v in os.environ.items() if k.startswith('GIT_')}, "
            "'token_available': bool(os.environ.get('CODECOV_TOKEN'))}\n"
            "pathlib.Path(os.environ['VALIDATION_CAPTURE_PATH']).write_text(json.dumps(data))\n"
            "hang = os.environ.get('VALIDATION_HANG')\n"
            "if hang:\n"
            "    if hang == 'partial-line':\n"
            "        print('Upload stalled without a newline', end='', flush=True)\n"
            "    elif hang == 'closed-output':\n"
            "        os.close(1)\n"
            "        os.close(2)\n"
            "    elif hang == 'descendant':\n"
            "        subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n"
            "        sys.exit(0)\n"
            "    time.sleep(30)\n"
            "print('Sending upload (1234 bytes) to storage')\n"
            "if os.environ.get('VALIDATION_FAIL'):\n"
            "    print('ExpiredToken https://storage.example/report?signature=secret-query ' "
            "+ os.environ['CODECOV_TOKEN'])\n"
            "    sys.exit(7)\n",
            encoding="utf-8",
        )
        self.cli.chmod(0o755)

    @staticmethod
    def run_git(directory: Path, *args: str) -> None:
        subprocess.run(
            ["git", "-C", str(directory), *args],
            env=upload_coverage.clean_environment(os.environ),
            check=True, capture_output=True,
        )

    def write(self, name: str, content: bytes = b"int extra();\n") -> Path:
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return path

    def write_report(self, names: list[str], newline: str = "\n") -> None:
        text = "".join(
            f"TN:{newline}SF:{name}{newline}DA:1,1{newline}end_of_record{newline}"
            for name in names
        )
        self.report.write_bytes(text.encode())

    def arguments(self, *extra: str) -> list[str]:
        return [
            "--source-root", str(self.root), "--suite", "cli", "--report", str(self.report),
            "--temp-dir", str(self.temp_dir), "--cli", str(self.cli), "--",
            "--fail-on-error", "--git-service", "github", "--slug", "owner/repository",
            "--sha", "a" * 40, "--branch", "feature", "--flag", "cli",
            "--name", "cli-123-2", "--pull-request-number", "456", *extra,
        ]

    def invoke(self, *extra: str, fail: bool = False, hang: str = "") -> tuple[int, str]:
        env = {
            "VALIDATION_CAPTURE_PATH": str(self.capture),
            "GITHUB_STEP_SUMMARY": str(self.summary),
            "CODECOV_TOKEN": "secret-repository-token",
            "GIT_DIR": str(self.root / ".git"),
            "GIT_WORK_TREE": str(self.root),
            "GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "core.worktree",
            "GIT_CONFIG_VALUE_0": str(self.root),
            "VALIDATION_FAIL": "1" if fail else "",
            "VALIDATION_HANG": hang,
        }
        output = io.StringIO()
        with mock.patch.dict(os.environ, env), contextlib.redirect_stdout(output):
            code = upload_coverage.main(self.arguments(*extra))
        return code, output.getvalue()

    def test_sources_accept_absolute_relative_duplicate_and_crlf_paths(self) -> None:
        names = list(self.sources)
        self.write_report([str(self.root / names[0]), names[1], names[0]], newline="\r\n")
        before = self.report.read_bytes()
        selected = upload_coverage.source_files(self.report, self.root, "cli")
        self.assertEqual(selected, {name: self.root / name for name in sorted(names)})
        self.assertEqual(self.report.read_bytes(), before)

    def test_staging_copies_exactly_measured_files_and_preserves_contents(self) -> None:
        stage = self.directory / "stage"
        stage.mkdir()
        selected = upload_coverage.source_files(self.report, self.root, "cli")
        upload_coverage.stage_sources(selected, stage)
        actual = {p.relative_to(stage).as_posix(): p.read_bytes()
                  for p in stage.rglob("*") if p.is_file()}
        self.assertEqual(actual, self.sources)
        self.assertFalse(any(p.is_symlink() for p in stage.rglob("*")))
        self.assertFalse((stage / ".git").exists())

    def test_rejects_invalid_outside_suite_missing_and_untracked_sources(self) -> None:
        self.write("ydb/apps/ydb/untracked.cpp")
        cases = [
            "", "../checkout/ydb/apps/ydb/main.cpp", "ydb/apps/ydb/../main.cpp",
            "ydb/apps/ydb/.git/config", "ydb/apps/ydb/.GIT/config",
            "ydb/apps/ydb/a\\b.cpp", "ydb/apps/ydb/control\x01.cpp",
            str(self.directory / "outside.cpp"), "ydb/apps/ydb/missing.cpp",
            "ydb/apps/ydb/untracked.cpp", "ydb/apps/ydb",
            "ydb/public/sdk/cpp/client.cpp", "ydb/library/backup/backup.cpp",
            "unrelated/huge.cpp", ".agents/private.txt",
        ]
        for name in cases:
            with self.subTest(name=name):
                self.write_report([name])
                with self.assertRaises(upload_coverage.UploadError):
                    upload_coverage.source_files(self.report, self.root, "cli")

    def test_rejects_empty_report(self) -> None:
        self.report.write_text("TN:\nend_of_record\n", encoding="utf-8")
        with self.assertRaisesRegex(upload_coverage.UploadError, "no source"):
            upload_coverage.source_files(self.report, self.root, "cli")

    def test_rejects_symlink_sources_and_symlink_parent_directories(self) -> None:
        target = self.root / "ydb/apps/ydb/main.cpp"
        for destination in (target, self.directory / "outside.cpp"):
            if not destination.exists():
                destination.write_text("int outside();\n", encoding="utf-8")
            link = self.root / "ydb/apps/ydb/link.cpp"
            link.symlink_to(destination)
            self.run_git(self.root, "add", str(link))
            self.write_report([str(link)])
            with self.assertRaises(upload_coverage.UploadError):
                upload_coverage.source_files(self.report, self.root, "cli")
            link.unlink()
        directory_link = self.root / "ydb/apps/ydb/alias"
        directory_link.symlink_to(target.parent, target_is_directory=True)
        self.write_report([str(directory_link / "main.cpp")])
        with self.assertRaises(upload_coverage.UploadError):
            upload_coverage.source_files(self.report, self.root, "cli")

    def test_rejects_nonregular_file(self) -> None:
        fifo = self.root / "ydb/apps/ydb/pipe.cpp"
        os.mkfifo(fifo)
        self.write_report([str(fifo)])
        with self.assertRaises(upload_coverage.UploadError):
            upload_coverage.source_files(self.report, self.root, "cli")

    def test_fake_cli_receives_exact_scope_metadata_config_and_clean_environment(self) -> None:
        before = self.report.read_bytes()
        code, output = self.invoke()
        self.assertEqual(code, 0, output)
        captured = json.loads(self.capture.read_text(encoding="utf-8"))
        self.assertEqual(captured["files"], {name: data.hex() for name, data in self.sources.items()})
        self.assertEqual(captured["git_env"], {})
        self.assertTrue(captured["token_available"])
        args = captured["args"]
        self.assertEqual(args[:4], ["--disable-telem", "--codecov-yml-path",
                                   str(self.root / ".github/codecov.yml"), "upload-coverage"])
        for option, value in {
            "--sha": "a" * 40, "--branch": "feature", "--flag": "cli",
            "--slug": "owner/repository", "--name": "cli-123-2",
            "--pull-request-number": "456", "--file": str(self.report),
            "--plugin": "noop", "--network-root-folder": captured["cwd"],
        }.items():
            self.assertEqual(args[args.index(option) + 1], value)
        self.assertIn("--disable-search", args)
        self.assertNotIn("--disable-file-fixes", args)
        self.assertEqual(self.report.read_bytes(), before)
        self.assertFalse(Path(captured["cwd"]).exists())
        self.assertEqual(list(self.temp_dir.iterdir()), [])
        self.assertIn("1234 bytes", self.summary.read_text(encoding="utf-8"))
        self.assertIn("success", self.summary.read_text(encoding="utf-8"))

    def test_failure_cleans_up_redacts_secrets_and_explains_expired_token(self) -> None:
        code, output = self.invoke(fail=True)
        self.assertEqual(code, 7, output)
        self.assertIn("::error title=Codecov publication::", output)
        self.assertIn("ExpiredToken", output)
        self.assertIn("Tests were not rerun", output)
        self.assertNotIn("secret-repository-token", output)
        self.assertNotIn("secret-query", output)
        self.assertIn("?[REDACTED]", output)
        summary = self.summary.read_text(encoding="utf-8")
        self.assertIn("failed", summary)
        self.assertIn("expired upload URL", summary)
        self.assertFalse(Path(json.loads(self.capture.read_text())["cwd"]).exists())
        self.assertEqual(list(self.temp_dir.iterdir()), [])

    def test_refuses_temporary_directory_inside_this_or_another_git_checkout(self) -> None:
        other = self.directory / "other-checkout"
        other.mkdir()
        self.run_git(other, "init", "-q")
        for parent in (self.root, other):
            with self.subTest(parent=parent):
                self.temp_dir = parent
                code, output = self.invoke()
                self.assertEqual(code, 1)
                self.assertIn("outside every Git repository", output)
                self.assertFalse(self.capture.exists())
                self.assertEqual(list(parent.glob("codecov-upload-*")), [])

    def test_timeout_interrupts_output_reads_wait_and_inherited_output(self) -> None:
        for hang in ("silent", "partial-line", "closed-output", "descendant"):
            with self.subTest(hang=hang), mock.patch.object(upload_coverage, "UPLOAD_TIMEOUT_SECONDS", 1):
                before = self.report.read_bytes()
                started = time.monotonic()
                code, output = self.invoke(hang=hang)
                self.assertLess(time.monotonic() - started, 10)
                self.assertEqual(code, 124, output)
                self.assertRegex(output, r"(?m)^::error title=Codecov publication::")
                self.assertIn("Codecov CLI timed out after 1s", output)
                self.assertIn("Tests were not rerun", output)
                self.assertIn("failed", self.summary.read_text(encoding="utf-8"))
                self.assertIn("timed out", self.summary.read_text(encoding="utf-8"))
                self.assertEqual(self.report.read_bytes(), before)
                self.assertFalse(Path(json.loads(self.capture.read_text())["cwd"]).exists())
                self.assertEqual(list(self.temp_dir.iterdir()), [])
                if hang == "partial-line":
                    self.assertIn("Upload stalled without a newline", output)

    def test_reserved_selection_arguments_cannot_broaden_scope(self) -> None:
        for option in (
            "--file=other.lcov", "-f", "-fother.lcov", "-f/path/to/other.lcov", "-f=other.lcov",
            "--network-root-folder=/", "--network-prefix=x",
            "--network-filter=y", "--plugin=gcov", "--disable-file-fixes",
            "--disable-search", "--recurse-submodules",
            "--files-search-direct-file", "--coverage-files-search-direct-file",
        ):
            with self.subTest(option=option):
                code, output = self.invoke(option)
                self.assertEqual(code, 1)
                self.assertIn("managed by this helper", output)
                self.assertFalse(self.capture.exists())

    def test_environment_removes_all_git_overrides_without_losing_upload_token(self) -> None:
        self.assertEqual(upload_coverage.clean_environment({
            "GIT_DIR": "repo", "GIT_COMMON_DIR": "repo", "GIT_CONFIG_KEY_0": "x",
            "GIT_CEILING_DIRECTORIES": "/", "CODECOV_TOKEN": "token", "PATH": "bin",
        }), {"CODECOV_TOKEN": "token", "PATH": "bin"})

    def test_output_redaction_preserves_useful_error_context(self) -> None:
        with mock.patch.dict(os.environ, {"CODECOV_TOKEN": "repository-secret"}):
            text = upload_coverage.safe_output(
                "ExpiredToken https://storage.example/report?X-Signature=abcdef&token=q "
                "repository-secret; https://codecov.io/path without query"
            )
        self.assertEqual(text, "ExpiredToken https://storage.example/report?[REDACTED] "
                         "[REDACTED]; https://codecov.io/path without query")


if __name__ == "__main__":
    unittest.main()
