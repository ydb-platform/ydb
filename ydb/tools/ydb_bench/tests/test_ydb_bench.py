import hashlib
import io
import json
import os
import signal
import stat
import tempfile
import textwrap
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from ydb.tools.ydb_bench.lib.actors_core import (
    CSV_HEADER,
    RunConfiguration,
    parse_metrics,
    run_actors_core,
)
from ydb.tools.ydb_bench.lib.common import BenchmarkError, BenchmarkInterrupted, extract_executable
from ydb.tools.ydb_bench.lib.cli import main
from ydb.tools.ydb_bench.lib.runner import run_command
from ydb.tools.ydb_bench.lib.topology import (
    AFFINITY_MODES,
    CpuTopology,
    discover_topology,
    parse_cpu_list,
    plan_affinity,
)


class YdbBenchTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory(prefix="ydb-bench-test-")
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

    def test_timeout_rejects_non_finite_values(self):
        for value in ("nan", "inf", "-inf"):
            with self.subTest(value=value):
                error = io.StringIO()
                with redirect_stderr(error), self.assertRaises(SystemExit) as raised:
                    main(["run", "actors-core", "--timeout={}".format(value)])
                self.assertEqual(raised.exception.code, 2)
                self.assertIn("must be a finite positive number", error.getvalue())

    def test_cli_exit_code_uses_interruption_error_type(self):
        def loader_for(error):
            def loader(_):
                raise error

            return loader

        error_output = io.StringIO()
        with redirect_stderr(error_output):
            generic_code = main(
                ["run", "actors-core", "--output", str(self.root / "generic-error")],
                resource_loader=loader_for(BenchmarkError("benchmark was interrupted by another component")),
            )
            interrupted_code = main(
                ["run", "actors-core", "--output", str(self.root / "interrupted-error")],
                resource_loader=loader_for(BenchmarkInterrupted("benchmark stopped")),
            )
        self.assertEqual(generic_code, 1)
        self.assertEqual(interrupted_code, 130)

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
        self.assertIn("none,1,32,1,3,1000.0,1000.0,1000.0,1.0", (output / "summary.csv").read_text())
        stored = json.loads((output / "run.json").read_text())
        self.assertEqual(stored["schema_version"], 2)
        self.assertEqual(stored["affinity"][0]["mode"], "none")
        self.assertEqual(stored["binary"]["sha256"], self._binary(script).sha256)
        for index in range(1, 4):
            repetition = output / "none" / "repeat-{:03d}".format(index)
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

    def test_start_failure_finalizes_manifest(self):
        script = self._script("exit 0")
        binary = self._binary(script)
        binary.path.chmod(0o644)
        output = self.root / "start-failure-output"
        output.mkdir()

        with self.assertRaisesRegex(BenchmarkError, "noexec"):
            run_actors_core(
                binary,
                self._configuration(),
                output,
                tool_revision={"commit_id": "test"},
                work_dir_hint=self.root,
            )

        manifest = json.loads((output / "run.json").read_text())
        self.assertEqual(manifest["status"], "failed")
        self.assertIn("finished_at", manifest)
        self.assertIn("noexec", manifest["error"])
        self.assertEqual(len(manifest["runs"]), 1)
        self.assertIn("finished_at", manifest["runs"][0])
        self.assertIsNone(manifest["runs"][0]["exit_code"])
        self.assertEqual(manifest["runs"][0]["error"], manifest["error"])
        self.assertFalse((output / "none" / "repeat-001").exists())

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

    def test_cpu_list_parser(self):
        self.assertEqual(parse_cpu_list("0-3,8,10-11\n"), (0, 1, 2, 3, 8, 10, 11))

    def test_topology_discovery_intersects_sysfs_with_allowed_cpus(self):
        sys_root = self.root / "sys" / "devices" / "system"
        for node_id, cpus in ((0, "0-3"), (1, "4-7")):
            node = sys_root / "node" / "node{}".format(node_id)
            node.mkdir(parents=True)
            node.joinpath("cpulist").write_text(cpus, encoding="utf-8")
        for cpu, shared in ((0, "0-1"), (1, "0-1"), (2, "2-3"), (3, "2-3")):
            cache = sys_root / "cpu" / "cpu{}".format(cpu) / "cache" / "index3"
            cache.mkdir(parents=True)
            cache.joinpath("level").write_text("3", encoding="utf-8")
            cache.joinpath("shared_cpu_list").write_text(shared, encoding="utf-8")

        topology = discover_topology(sys_root, allowed_cpus=(1, 2, 3, 4))
        self.assertEqual(topology.allowed_cpus, (1, 2, 3, 4))
        self.assertEqual(topology.numa_nodes, ((0, (1, 2, 3)), (1, (4,))))
        self.assertEqual(topology.chiplets, ((0, (1,)), (0, (2, 3))))

    def test_affinity_modes_select_equal_size_deterministic_masks(self):
        topology = CpuTopology(
            allowed_cpus=tuple(range(8)),
            numa_nodes=((0, (0, 1, 2, 3)), (1, (4, 5, 6, 7))),
            chiplets=((0, (0, 1)), (0, (2, 3)), (1, (4, 5)), (1, (6, 7))),
        )
        with mock.patch.object(os, "sched_setaffinity", create=True):
            placements = {mode: plan_affinity(mode, topology, 2) for mode in AFFINITY_MODES}
        self.assertIsNone(placements["none"].cpus)
        self.assertEqual(placements["single-numa"].cpus, (0, 1))
        self.assertEqual(placements["multi-numa"].cpus, (0, 4))
        self.assertEqual(placements["single-chiplet"].cpus, (0, 1))
        self.assertEqual(placements["multi-chiplet"].cpus, (0, 2))

    def test_unavailable_affinity_mode_is_reported_not_guessed(self):
        topology = CpuTopology(
            allowed_cpus=(0, 1),
            numa_nodes=((0, (0, 1)),),
            chiplets=((0, (0, 1)),),
        )
        with mock.patch.object(os, "sched_setaffinity", create=True):
            placement = plan_affinity("multi-numa", topology, 2)
        self.assertFalse(placement.supported)
        self.assertIn("two NUMA nodes", placement.reason)


if __name__ == "__main__":
    unittest.main()
