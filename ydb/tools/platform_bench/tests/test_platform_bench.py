import hashlib
import io
import json
import os
import signal
import stat
import tempfile
import textwrap
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from ydb.tools.platform_bench.lib.actors_core import (
    CSV_HEADER,
    RunConfiguration,
    parse_metrics,
    run_actors_core,
)
from ydb.tools.platform_bench.lib.common import BenchmarkError, extract_executable
from ydb.tools.platform_bench.lib.cli import main
from ydb.tools.platform_bench.lib.runner import run_command


class PlatformBenchTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory(prefix="platform-bench-test-")
        self.root = Path(self.temporary_directory.name)

    def tearDown(self):
        self.temporary_directory.cleanup()

    def _script(self, body, name="fake_benchmark.sh"):
        path = self.root / name
        path.write_text("#!/bin/sh\n{}".format(textwrap.dedent(body)), encoding="utf-8")
        path.chmod(0o755)
        return path

    def _binary(self, path):
        data = path.read_bytes()
        return extract_executable(data, self.root / "extracted", "actors_core_ut_fat")

    def _configuration(self, repetitions=1, timeout=5):
        return RunConfiguration(
            profile="test",
            threads=(1, 2),
            actor_pairs=(32,),
            inflights=(1,),
            duration_seconds=1,
            repetitions=repetitions,
            timeout_seconds=timeout,
        )

    def test_extract_executable_is_atomic_executable_and_hashed(self):
        data = b"#!/bin/sh\nexit 0\n"
        artifact = extract_executable(data, self.root / "bin", "test-binary")
        self.assertEqual(artifact.sha256, hashlib.sha256(data).hexdigest())
        self.assertEqual(artifact.size, len(data))
        self.assertEqual(artifact.path.read_bytes(), data)
        self.assertTrue(artifact.path.stat().st_mode & stat.S_IXUSR)
        self.assertEqual(list(artifact.path.parent.glob(".test-binary.*")), [])

    def test_parse_metrics_ignores_unittest_output(self):
        stdout = "\n".join(
            [
                "[==========] Running 1 test",
                CSV_HEADER,
                "1,32,1,1000,1.5,900,1100",
                "[       OK ] HeavyActorBenchmark::SendActivateReceiveCSVManual",
            ]
        )
        self.assertEqual(parse_metrics(stdout)[0]["msgs_per_sec"], 1000)

    def test_parse_metrics_rejects_header_without_rows(self):
        with self.assertRaisesRegex(BenchmarkError, "no metric rows"):
            parse_metrics(CSV_HEADER + "\n[       OK ]")

    def test_list_and_describe_expose_actors_core(self):
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(main(["list"]), 0)
            self.assertEqual(main(["describe", "actors-core"]), 0)
        self.assertIn("actors-core", output.getvalue())
        self.assertIn("summary.csv", output.getvalue())

    def test_run_writes_manifest_raw_metrics_and_median_summary(self):
        script = self._script(
            """
            test "$1" = "HeavyActorBenchmark::SendActivateReceiveCSVManual" || exit 10
            test "$ACTORSYSTEM_TEST_MODE" = "manual" || exit 11
            test "$ACTORSYSTEM_THREADS" = "1,2" || exit 12
            test "$ACTORSYSTEM_ACTOR_PAIRS" = "32" || exit 13
            test "$ACTORSYSTEM_INFLIGHTS" = "1" || exit 14
            test "$ACTORSYSTEM_DURATION" = "1" || exit 15
            echo "[ RUN      ] benchmark"
            echo "threads,actorPairs,in_flight,msgs_per_sec,elapsed_seconds,min_pair_sent_msgs,max_pair_sent_msgs"
            echo "1,32,1,1000,1.0,900,1100"
            echo "2,32,1,2000,1.0,1800,2200"
            """
        )
        output = self.root / "output"
        output.mkdir()
        manifest = run_actors_core(
            self._binary(script),
            self._configuration(repetitions=3),
            output,
            tool_revision={"commit_id": "test"},
            work_dir_hint=self.root,
        )
        self.assertEqual(manifest["status"], "completed")
        self.assertEqual(len(manifest["runs"]), 3)
        self.assertTrue((output / "summary.csv").is_file())
        self.assertIn("1,32,1,3,1000.0,1000.0,1000.0,1.0", (output / "summary.csv").read_text())
        stored = json.loads((output / "run.json").read_text())
        self.assertEqual(stored["binary"]["sha256"], self._binary(script).sha256)
        for index in range(1, 4):
            repetition = output / "repeat-{:03d}".format(index)
            self.assertTrue((repetition / "stdout.txt").is_file())
            self.assertTrue((repetition / "stderr.txt").is_file())
            self.assertTrue((repetition / "metrics.csv").is_file())

    def test_empty_csv_fails_even_with_zero_exit_code(self):
        script = self._script(
            """
            echo "threads,actorPairs,in_flight,msgs_per_sec,elapsed_seconds,min_pair_sent_msgs,max_pair_sent_msgs"
            """
        )
        output = self.root / "empty-output"
        output.mkdir()
        with self.assertRaisesRegex(BenchmarkError, "no metric rows"):
            run_actors_core(
                self._binary(script),
                self._configuration(),
                output,
                tool_revision={"commit_id": "test"},
            )
        manifest = json.loads((output / "run.json").read_text())
        self.assertEqual(manifest["status"], "failed")

    @unittest.skipUnless(hasattr(os, "killpg"), "requires POSIX process groups")
    def test_timeout_signals_the_whole_process_group(self):
        marker = self.root / "child-terminated"
        script = self._script(
            """
            marker="$1"
            (
                trap 'echo 15 > "$marker"; exit 0' TERM
                while :; do
                    sleep 1
                done
            ) &
            trap 'wait; exit 0' TERM
            while :; do
                sleep 1
            done
            """,
            name="process_tree.sh",
        )
        result = run_command(
            [script, marker],
            {},
            timeout_seconds=0.5,
            grace_seconds=2,
        )
        self.assertTrue(result.timed_out)
        self.assertEqual(marker.read_text().strip(), str(int(signal.SIGTERM)))

    def test_permission_error_mentions_noexec_and_work_dir(self):
        path = self.root / "not-executable"
        path.write_text("#!/bin/sh\n")
        path.chmod(0o644)
        with self.assertRaisesRegex(BenchmarkError, "noexec.*--work-dir"):
            run_command([path], {}, timeout_seconds=1, work_dir_hint=self.root)


if __name__ == "__main__":
    unittest.main()
