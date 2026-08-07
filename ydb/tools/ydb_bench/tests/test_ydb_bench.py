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
    PING_BENCHMARK,
    STAR_PING_BENCHMARK,
    RunConfiguration,
    parse_metrics,
    run_actors_core,
)
from ydb.tools.ydb_bench.lib.cli import main
from ydb.tools.ydb_bench.lib.common import BenchmarkError, BenchmarkInterrupted, extract_executable
from ydb.tools.ydb_bench.lib.config import CONFIG_SCHEMA, load_config
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

    def _config(self, body, name="bench.yaml"):
        path = self.root / name
        path.write_text(textwrap.dedent(body), encoding="utf-8")
        return path

    def _configuration(self, repetitions=1, timeout=5, benchmark=PING_BENCHMARK):
        return RunConfiguration(
            benchmark=benchmark,
            profile="test",
            threads=(1, 2),
            actor_pairs=(32,),
            parameter_values=(1,),
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
                PING_BENCHMARK.csv_header,
                "1,32,1,1000,1.5,900,1100",
                "[       OK ] HeavyActorBenchmark::SendActivateReceiveCSVManual",
            ]
        )
        self.assertEqual(parse_metrics(stdout)[0]["msgs_per_sec"], 1000)

    def test_parse_star_metrics_uses_star_column(self):
        """Parse the star CSV header first, then verify its benchmark-specific value column."""
        stdout = "\n".join([STAR_PING_BENCHMARK.csv_header, "1,32,4,1000,1.5,900,1100"])
        rows = parse_metrics(stdout, STAR_PING_BENCHMARK)
        self.assertEqual(rows[0]["star_multiply"], 4)

    def test_parse_metrics_rejects_header_without_rows(self):
        with self.assertRaisesRegex(BenchmarkError, "no metric rows"):
            parse_metrics(PING_BENCHMARK.csv_header + "\n[       OK ]")

    def test_list_describe_and_config_schema_expose_current_contract(self):
        """List benchmarks, describe ping then star, and finally validate the printed schema."""
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(main(["list"]), 0)
            self.assertEqual(main(["describe", "ping-bench"]), 0)
            self.assertEqual(main(["describe", "star-ping-bench"]), 0)
        description = output.getvalue()
        self.assertIn("ping-bench", description)
        self.assertIn("star-ping-bench", description)
        self.assertNotIn("smoke", description)

        schema_output = io.StringIO()
        with redirect_stdout(schema_output):
            self.assertEqual(main(["config-schema"]), 0)
        self.assertEqual(json.loads(schema_output.getvalue()), CONFIG_SCHEMA)
        self.assertEqual(set(CONFIG_SCHEMA["properties"]), {"ping-bench", "star-ping-bench"})

    def test_config_supports_multiple_benchmarks_and_profiles(self):
        """Load ping baseline, ping focused, then star sweep while preserving YAML order."""
        config = self._config(
            """
            ping-bench:
              baseline:
                threads: [1, 2, 4]
                duration: 3
                repetitions: 5
                affinity: [none]
              focused:
                threads: [16]
                actor-pairs: [1024]
                inflight: [2, 4]
                duration: 10
                repetitions: 1
                affinity: [one-whole-chiplet]
            star-ping-bench:
              star-sweep:
                threads: [8]
                stars: [1, 2, 4]
                duration: 4
                repetitions: 2
                affinity: [none, multi-chiplet]
            """
        )
        loaded = load_config(config, perf_enabled=True, perf_frequency=123)
        self.assertEqual(
            [(run.benchmark.name, run.profile) for run in loaded.runs],
            [
                ("ping-bench", "baseline"),
                ("ping-bench", "focused"),
                ("star-ping-bench", "star-sweep"),
            ],
        )
        self.assertEqual(loaded.runs[0].actor_pairs, (512,))
        self.assertEqual(loaded.runs[0].parameter_values, (1,))
        self.assertEqual(loaded.runs[1].parameter_values, (2, 4))
        self.assertEqual(loaded.runs[2].parameter_values, (1, 2, 4))
        self.assertTrue(all(run.perf_enabled for run in loaded.runs))
        self.assertTrue(all(run.perf_frequency == 123 for run in loaded.runs))

    def test_config_rejects_empty_arrays_unknown_fields_and_unsafe_profile_names(self):
        """Reject empty threads, then an unknown field, then an unsafe profile path."""
        cases = (
            (
                "empty-threads.yaml",
                """
                ping-bench:
                  baseline:
                    threads: []
                    duration: 1
                    repetitions: 1
                    affinity: [none]
                """,
                "non-empty array",
            ),
            (
                "unknown-field.yaml",
                """
                ping-bench:
                  baseline:
                    threads: [1]
                    duration: 1
                    repetitions: 1
                    affinity: [none]
                    surprise: 42
                """,
                "unknown fields: surprise",
            ),
            (
                "unsafe-name.yaml",
                """
                ping-bench:
                  ../escape:
                    threads: [1]
                    duration: 1
                    repetitions: 1
                    affinity: [none]
                """,
                "profile names must match",
            ),
        )
        for name, body, error in cases:
            with self.subTest(name=name), self.assertRaisesRegex(BenchmarkError, error):
                load_config(self._config(body, name=name))

    def test_config_rejects_non_finite_timeout(self):
        """Reject NaN, positive infinity, then negative infinity as profile timeouts."""
        for value in (".nan", ".inf", "-.inf"):
            with self.subTest(value=value), self.assertRaisesRegex(BenchmarkError, "finite positive number"):
                load_config(
                    self._config(
                        """
                        ping-bench:
                          baseline:
                            threads: [1]
                            duration: 1
                            repetitions: 1
                            affinity: [none]
                            timeout: {}
                        """.format(value),
                        name="timeout-{}.yaml".format(value.replace("/", "_")),
                    )
                )

    def test_config_rejects_duplicate_yaml_keys(self):
        """Parse a profile with duplicate threads keys and reject it before normalization."""
        config = self._config(
            """
            ping-bench:
              baseline:
                threads: [1]
                threads: [2]
                duration: 1
                repetitions: 1
                affinity: [none]
            """
        )
        with self.assertRaisesRegex(BenchmarkError, "duplicate key 'threads'"):
            load_config(config)

    def test_perf_requires_profile_build(self):
        config = self._config(
            """
            ping-bench:
              baseline:
                threads: [1]
                duration: 1
                repetitions: 1
                affinity: [none]
            """
        )
        error = io.StringIO()
        with redirect_stderr(error):
            code = main(
                [
                    "run",
                    "--config",
                    str(config),
                    "--perf",
                    "--output",
                    str(self.root / "non-profile"),
                ],
                resource_loader=lambda _: b"fake",
                tool_revision={"build_type": "relwithdebinfo", "commit_id": "test"},
            )
        self.assertEqual(code, 1)
        self.assertIn("--build=profile", error.getvalue())
        self.assertFalse((self.root / "non-profile").exists())

    def test_cli_runs_multiple_benchmarks_and_profiles(self):
        """Run ping-bench/first, ping-bench/second, then star-ping-bench/star with separate summaries."""
        benchmark = self._script(
            """
            test "$ACTORSYSTEM_TEST_MODE" = "manual" || exit 10
            test "$ACTORSYSTEM_THREADS" = "1" || exit 11
            test "$ACTORSYSTEM_ACTOR_PAIRS" = "32" || exit 12
            case "$1" in
              HeavyActorBenchmark::SendActivateReceiveCSVManual)
                test "$ACTORSYSTEM_INFLIGHTS" = "1" || exit 13
                echo "threads,actorPairs,in_flight,msgs_per_sec,elapsed_seconds,min_pair_sent_msgs,max_pair_sent_msgs"
                echo "1,32,1,1000,1.0,900,1100"
                ;;
              HeavyActorBenchmark::StarSendActivateReceiveCSVManual)
                test "$ACTORSYSTEM_STARS" = "2" || exit 14
                test "$ACTORSYSTEM_DURATION" = "2" || exit 15
                printf '%s%s\n' \
                  "threads,actorPairs,star_multiply,msgs_per_sec,elapsed_seconds," \
                  "min_pair_sent_msgs,max_pair_sent_msgs"
                echo "1,32,2,2000,2.0,1800,2200"
                ;;
              *) exit 16 ;;
            esac
            """
        )
        config = self._config(
            """
            ping-bench:
              first:
                threads: [1]
                actor-pairs: [32]
                duration: 1
                repetitions: 1
                affinity: [none]
              second:
                threads: [1]
                actor-pairs: [32]
                duration: 1
                repetitions: 1
                affinity: [none]
            star-ping-bench:
              star:
                threads: [1]
                actor-pairs: [32]
                stars: [2]
                duration: 2
                repetitions: 1
                affinity: [none]
            """
        )
        output = self.root / "multi-output"
        console = io.StringIO()
        with redirect_stdout(console):
            code = main(
                ["run", "--config", str(config), "--output", str(output)],
                resource_loader=lambda _: benchmark.read_bytes(),
                tool_revision={"build_type": "relwithdebinfo", "commit_id": "test"},
            )
        self.assertEqual(code, 0)
        manifest = json.loads((output / "run.json").read_text())
        self.assertEqual(manifest["status"], "completed")
        self.assertEqual(len(manifest["runs"]), 3)
        self.assertTrue(all(run["status"] == "completed" for run in manifest["runs"]))
        self.assertEqual(
            [run["summary"] for run in manifest["runs"]],
            [
                "ping-bench/first/summary.csv",
                "ping-bench/second/summary.csv",
                "star-ping-bench/star/summary.csv",
            ],
        )
        first = (output / "ping-bench" / "first" / "summary.csv").read_text()
        second = (output / "ping-bench" / "second" / "summary.csv").read_text()
        star = (output / "star-ping-bench" / "star" / "summary.csv").read_text()
        self.assertIn("in_flight", first.splitlines()[0])
        self.assertIn("none,1,32,1,1,1000.0", first)
        self.assertEqual(first, second)
        self.assertIn("star_multiply", star.splitlines()[0])
        self.assertIn("none,1,32,2,1,2000.0", star)
        self.assertFalse((output / "summary.csv").exists())
        self.assertNotIn("summary", manifest)
        self.assertIn("ping-bench/first: ping-bench/first/summary.csv", console.getvalue())
        self.assertIn("star-ping-bench/star: star-ping-bench/star/summary.csv", console.getvalue())

    def test_cli_exit_code_uses_interruption_error_type(self):
        config = self._config(
            """
            ping-bench:
              test:
                threads: [1]
                duration: 1
                repetitions: 1
                affinity: [none]
            """
        )

        def loader_for(error):
            def loader(_):
                raise error

            return loader

        error_output = io.StringIO()
        with redirect_stderr(error_output):
            generic_code = main(
                ["run", "--config", str(config), "--output", str(self.root / "generic-error")],
                resource_loader=loader_for(BenchmarkError("benchmark failed")),
            )
            interrupted_code = main(
                ["run", "--config", str(config), "--output", str(self.root / "interrupted-error")],
                resource_loader=loader_for(BenchmarkInterrupted("benchmark stopped")),
            )
        self.assertEqual(generic_code, 1)
        self.assertEqual(interrupted_code, 130)
        generic_manifest = json.loads((self.root / "generic-error" / "run.json").read_text())
        interrupted_manifest = json.loads((self.root / "interrupted-error" / "run.json").read_text())
        self.assertEqual(generic_manifest["status"], "failed")
        self.assertEqual(interrupted_manifest["status"], "interrupted")

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
        self.assertEqual(stored["schema_version"], 3)
        self.assertEqual(stored["benchmark"], "ping-bench")
        self.assertEqual(stored["affinity"][0]["mode"], "none")
        self.assertEqual(stored["binary"]["sha256"], self._binary(script).sha256)
        for index in range(1, 4):
            repetition = output / "none" / "repeat-{:03d}".format(index)
            self.assertTrue((repetition / "stdout.txt").is_file())
            self.assertTrue((repetition / "stderr.txt").is_file())
            self.assertTrue((repetition / "metrics.csv").is_file())

    def test_star_run_selects_star_filter_environment_and_summary(self):
        """Select the star filter, pass stars and duration, then render a star-specific summary."""
        script = self._script(
            """
            test "$1" = "HeavyActorBenchmark::StarSendActivateReceiveCSVManual" || exit 10
            test "$ACTORSYSTEM_STARS" = "2,4" || exit 11
            test "$ACTORSYSTEM_DURATION" = "3" || exit 12
            echo "threads,actorPairs,star_multiply,msgs_per_sec,elapsed_seconds,min_pair_sent_msgs,max_pair_sent_msgs"
            echo "1,32,2,1000,3.0,900,1100"
            echo "1,32,4,2000,3.0,1800,2200"
            echo "2,32,2,3000,3.0,2800,3200"
            echo "2,32,4,4000,3.0,3800,4200"
            """
        )
        output = self.root / "star-output"
        output.mkdir()
        configuration = RunConfiguration(
            **{
                **self._configuration(benchmark=STAR_PING_BENCHMARK).__dict__,
                "parameter_values": (2, 4),
                "duration_seconds": 3,
            }
        )
        manifest = run_actors_core(
            self._binary(script),
            configuration,
            output,
            tool_revision={"commit_id": "test"},
        )
        self.assertEqual(manifest["benchmark"], "star-ping-bench")
        self.assertEqual(manifest["parameters"]["stars"], [2, 4])
        self.assertIn("star_multiply", (output / "summary.csv").read_text().splitlines()[0])

    def test_perf_run_preserves_binary_data_report_and_buildids(self):
        benchmark = self._script(
            """
            echo "threads,actorPairs,in_flight,msgs_per_sec,elapsed_seconds,min_pair_sent_msgs,max_pair_sent_msgs"
            echo "1,32,1,1000,1.0,900,1100"
            echo "2,32,1,2000,1.0,1800,2200"
            """
        )
        fake_perf = self._script(
            """
            subcommand="$1"
            shift
            case "$subcommand" in
              record)
                while [ "$1" != "--" ]; do
                  if [ "$1" = "-o" ]; then
                    shift
                    output="$1"
                  fi
                  shift
                done
                shift
                echo fake-perf-data > "$output"
                exec "$@"
                ;;
              report)
                echo "42.00% actors_core_ut_fat HotFunction"
                ;;
              buildid-list)
                echo "0123456789abcdef actors_core_ut_fat"
                ;;
              *)
                exit 90
                ;;
            esac
            """,
            name="perf",
        )
        output = self.root / "perf-output"
        output.mkdir()
        configuration = self._configuration()
        configuration = RunConfiguration(
            **{
                **configuration.__dict__,
                "perf_enabled": True,
                "perf_frequency": 123,
            }
        )
        path = os.environ.get("PATH", "")
        with mock.patch.dict(os.environ, {"PATH": "{}{}{}".format(fake_perf.parent, os.pathsep, path)}):
            manifest = run_actors_core(
                self._binary(benchmark),
                configuration,
                output,
                tool_revision={"build_type": "profile", "commit_id": "test"},
                work_dir_hint=self.root,
            )

        self.assertEqual(manifest["status"], "completed")
        self.assertEqual(manifest["profiler"]["event"], "cycles:u")
        self.assertEqual(manifest["profiler"]["frequency_hz"], 123)
        self.assertEqual(manifest["binary"]["artifact"], "profiler/actors_core_ut_fat")
        self.assertEqual(
            (output / manifest["binary"]["artifact"]).read_bytes(),
            benchmark.read_bytes(),
        )
        repetition = output / "none" / "repeat-001"
        self.assertTrue((repetition / "perf.data").is_file())
        self.assertIn("HotFunction", (repetition / "perf-report.txt").read_text())
        self.assertIn("0123456789abcdef", (repetition / "perf-buildids.txt").read_text())
        run = manifest["runs"][0]
        self.assertEqual(run["perf_data"], "none/repeat-001/perf.data")
        self.assertEqual([record["name"] for record in run["perf_postprocessing"]], ["report", "buildid-list"])

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

    def test_affinity_modes_select_deterministic_masks(self):
        topology = CpuTopology(
            allowed_cpus=tuple(range(8)),
            numa_nodes=((0, (0, 1, 2, 3)), (1, (4, 5, 6, 7))),
            chiplets=((0, (0, 1)), (0, (2, 3)), (1, (4, 5)), (1, (6, 7))),
        )
        with mock.patch.object(os, "sched_setaffinity", create=True):
            placements = {mode: plan_affinity(mode, topology, 2) for mode in AFFINITY_MODES}
        self.assertIsNone(placements["none"].cpus)
        self.assertEqual(placements["one-whole-numa"].cpus, (0, 1, 2, 3))
        self.assertEqual(placements["one-whole-chiplet"].cpus, (0, 1))
        self.assertEqual(placements["multi-chiplet"].cpus, (0, 2))

    def test_whole_affinity_modes_do_not_limit_masks_to_requested_threads(self):
        topology = CpuTopology(
            allowed_cpus=tuple(range(8)),
            numa_nodes=((0, (0, 1, 2, 3)), (1, (4, 5, 6, 7))),
            chiplets=((0, (0, 1, 2)), (0, (3,)), (1, (4, 5)), (1, (6, 7))),
        )
        with mock.patch.object(os, "sched_setaffinity", create=True):
            numa = plan_affinity("one-whole-numa", topology, 2)
            chiplet = plan_affinity("one-whole-chiplet", topology, 2)
        self.assertEqual(numa.cpus, (0, 1, 2, 3))
        self.assertEqual(chiplet.cpus, (0, 1, 2))

    def test_unavailable_affinity_mode_is_reported_not_guessed(self):
        topology = CpuTopology(
            allowed_cpus=(0, 1),
            numa_nodes=((0, (0, 1)),),
            chiplets=((0, (0, 1)),),
        )
        with mock.patch.object(os, "sched_setaffinity", create=True):
            placement = plan_affinity("multi-chiplet", topology, 2)
        self.assertFalse(placement.supported)
        self.assertIn("two chiplets", placement.reason)

    def test_run_fails_when_all_affinity_modes_are_unsupported(self):
        script = self._script("exit 99")
        output = self.root / "unsupported-output"
        output.mkdir()
        topology = CpuTopology(
            allowed_cpus=(0, 1),
            numa_nodes=((0, (0, 1)),),
            chiplets=((0, (0, 1)),),
        )
        configuration = RunConfiguration(
            benchmark=PING_BENCHMARK,
            profile="test",
            threads=(1, 2),
            actor_pairs=(32,),
            parameter_values=(1,),
            duration_seconds=1,
            repetitions=1,
            timeout_seconds=5,
            affinity_modes=("multi-chiplet",),
        )

        with mock.patch(
            "ydb.tools.ydb_bench.lib.actors_core.discover_topology", return_value=topology
        ), mock.patch.object(os, "sched_setaffinity", create=True), self.assertRaisesRegex(
            BenchmarkError, "none of the selected affinity modes is supported"
        ):
            run_actors_core(
                self._binary(script),
                configuration,
                output,
                tool_revision={"commit_id": "test"},
            )

        manifest = json.loads((output / "run.json").read_text())
        self.assertEqual(manifest["status"], "failed")
        self.assertIn("finished_at", manifest)
        self.assertIn("multi-chiplet", manifest["error"])
        self.assertEqual(manifest["runs"], [])
        self.assertEqual(manifest["affinity"][0]["status"], "unsupported")
        self.assertFalse((output / "summary.csv").exists())


if __name__ == "__main__":
    unittest.main()
