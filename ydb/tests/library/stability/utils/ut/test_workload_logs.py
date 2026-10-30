"""The workload wrapper must not ship the log back to the test process."""
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from ydb.tests.library.stability.utils.workload_logs import (
    TAIL_BYTES,
    build_redirected_workload_command,
    make_log_id,
    parse_workload_trailer,
)


def _run(script: str, env: dict | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        check=False,
    )


class WorkloadLogWrapperTest(unittest.TestCase):
    def test_clean_success_drops_logs_and_keeps_trailer_small(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = str(Path(tmp) / "logs with space")
            script, stdout_path, stderr_path = build_redirected_workload_command(
                "python3 -c 'print(\"A\"*2000000)'; "
                "echo 'Warning: Permanently added x' >&2; "
                "echo '(!) New version of YDB CL 1' >&2",
                "ok_run",
                log_dir=log_dir,
            )
            completed = _run(script)
            trailer = completed.stdout.decode("utf-8", errors="replace")
            info = parse_workload_trailer(trailer)
            logs_left = Path(stdout_path).exists() or Path(stderr_path).exists()

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertTrue(info.parsed)
        self.assertFalse(info.stderr_nonempty)
        self.assertFalse(info.logs_kept)
        self.assertEqual(info.stdout_tail, "")
        self.assertEqual(info.stderr_tail, "")
        self.assertGreater(info.stdout_bytes, 1_000_000)
        self.assertLess(len(trailer), 2000)
        self.assertNotIn("A" * 100, trailer)
        self.assertFalse(logs_left)

    def test_failure_keeps_files_and_returns_only_a_tail(self):
        stderr_len = TAIL_BYTES + 50_000
        with tempfile.TemporaryDirectory() as tmp:
            script, stdout_path, stderr_path = build_redirected_workload_command(
                "python3 -c 'import sys; sys.stdout.write(\"out\\n\"); "
                f"sys.stderr.write(\"E\"*{stderr_len})'; exit 7",
                "bad_run",
                log_dir=tmp,
            )
            completed = _run(script)
            trailer = completed.stdout.decode("utf-8", errors="replace")
            info = parse_workload_trailer(trailer)
            stderr_text = Path(stderr_path).read_text()
            stdout_text = Path(stdout_path).read_text()

        self.assertEqual(completed.returncode, 7, completed.stderr)
        self.assertTrue(info.stderr_nonempty)
        self.assertTrue(info.logs_kept)
        self.assertEqual(info.stderr_bytes, stderr_len)
        self.assertEqual(stderr_text, "E" * stderr_len)
        self.assertEqual(info.stderr_tail, "E" * TAIL_BYTES)
        self.assertEqual(info.stdout_tail, "out\n")
        self.assertEqual(stdout_text, "out\n")
        self.assertLess(len(trailer), TAIL_BYTES * 3)
        self.assertNotIn("E" * 100, trailer)

    def test_real_stderr_is_not_hidden_by_a_version_notice(self):
        with tempfile.TemporaryDirectory() as tmp:
            script, _, stderr_path = build_redirected_workload_command(
                "echo '(!) New version of YDB CL 1' >&2; echo 'boom' >&2; exit 1",
                "mixed",
                log_dir=tmp,
            )
            completed = _run(script)
            info = parse_workload_trailer(completed.stdout.decode())
            log_kept = Path(stderr_path).is_file()

        self.assertEqual(completed.returncode, 1)
        self.assertTrue(info.stderr_nonempty)
        self.assertIn("boom", info.stderr_tail)
        self.assertTrue(log_kept)

    def test_unparsed_trailer(self):
        info = parse_workload_trailer("not a trailer\n")
        self.assertFalse(info.parsed)
        self.assertFalse(info.logs_kept)
        self.assertEqual(info.stdout_tail, "")

    def test_default_logs_land_in_home_tmp(self):
        home = tempfile.mkdtemp()
        try:
            script, stdout_path, _stderr_path = build_redirected_workload_command(
                "echo boom >&2; exit 4",
                "kept_run",
            )
            self.assertEqual(stdout_path, "$HOME/tmp/stability_workload_logs/kept_run.out")
            self.assertIn('mkdir -p "$HOME/tmp/stability_workload_logs"', script)
            env = os.environ.copy()
            env["HOME"] = home
            completed = _run(script, env)
            info = parse_workload_trailer(completed.stdout.decode())
            kept = Path(home) / "tmp" / "stability_workload_logs" / "kept_run.err"
            self.assertEqual(completed.returncode, 4, completed.stderr)
            self.assertTrue(info.logs_kept)
            self.assertEqual(info.stderr_path, str(kept))
            self.assertEqual(kept.read_text().strip(), "boom")
        finally:
            shutil.rmtree(home, ignore_errors=True)

    def test_log_id_is_filesystem_safe(self):
        log_id = make_log_id("olap host/name iter 1")
        self.assertNotIn("/", log_id)
        self.assertNotIn(" ", log_id)
        build_redirected_workload_command("true", log_id)
        with self.assertRaises(ValueError):
            build_redirected_workload_command("true", "../escape")


if __name__ == "__main__":
    unittest.main()
