import hashlib
import io
import json
import os
import signal
import stat
import tempfile
import textwrap
import threading
import time
import unittest
import urllib.request
import zipfile
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
from ydb.tools.ydb_bench.benchmarks.registry import BenchmarkDefinition, BenchmarkRegistry
from ydb.tools.ydb_bench.lib.cli import main
from ydb.tools.ydb_bench.lib.common import BenchmarkError, BenchmarkInterrupted, extract_executable
from ydb.tools.ydb_bench.lib.config import CONFIG_SCHEMA, build_run_plan, config_schema, load_config
from ydb.tools.ydb_bench.lib.results import ResultStore, SCHEMA_VERSION, load_manifest, transition
from ydb.tools.ydb_bench.lib.runner import run_command
from ydb.tools.ydb_bench.lib.topology import (
    CpuTopology,
    discover_topology,
    parse_cpu_list,
    plan_affinity,
    topology_record,
)
from ydb.tools.ydb_bench.lib.import_results import import_archive
from ydb.tools.ydb_bench.lib.web import comparison_keys, make_server, read_model


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

    def test_cli_json_discovery_and_validation_do_not_create_output(self):
        config = self._config(
            """
            ping-bench:
              invalid:
                threads: []
                duration: 1
                repetitions: 1
                affinity: [none]
            """
        )
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(main(["list", "--json"]), 0)
        listed = json.loads(output.getvalue())
        self.assertIn("defaults", listed[0])
        self.assertIn("affinity_modes", listed[0])
        self.assertIn("examples", listed[0])

        output, errors = io.StringIO(), io.StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            self.assertEqual(main(["validate", "--config", str(config), "--json"]), 1)
        result = json.loads(output.getvalue())
        self.assertEqual(result["error"]["path"], "ping-bench.invalid.threads")
        self.assertIn("non-empty", result["error"]["message"])
        self.assertEqual(errors.getvalue(), "")
        self.assertFalse((self.root / "output").exists())

    def test_cli_report_stdout_and_queue_error_policies(self):
        benchmark = self._script(
            """
            test "$ACTORSYSTEM_INFLIGHTS" = "2" || exit 23
            echo "threads,actorPairs,in_flight,msgs_per_sec,elapsed_seconds,min_pair_sent_msgs,max_pair_sent_msgs"
            echo "1,32,2,1000,1.0,900,1100"
            """
        )
        config = self._config(
            """
            ping-bench:
              fails: {threads: [1], actor-pairs: [32], inflight: [1], duration: 1, repetitions: 1, affinity: [none]}
              succeeds: {threads: [1], actor-pairs: [32], inflight: [2], duration: 1, repetitions: 1, affinity: [none]}
            """
        )

        def loader(_):
            return benchmark.read_bytes()
        fail_fast = self.root / "fail-fast"
        fail_fast_stdout, fail_fast_stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(fail_fast_stdout), redirect_stderr(fail_fast_stderr):
            self.assertEqual(main(["run", "--config", str(config), "--output", str(fail_fast)], loader), 1)
        self.assertEqual(len(json.loads((fail_fast / "run.json").read_text())["runs"]), 1)
        self.assertEqual(fail_fast_stdout.getvalue(), "")
        self.assertIn("failed 2 benchmark profiles: {}".format(fail_fast), fail_fast_stderr.getvalue())

        continued, stdout, stderr = self.root / "continued", io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            self.assertEqual(main(["run", "--config", str(config), "--output", str(continued), "--continue-on-error", "--report-json", "-"], loader), 1)
        report_payload = stdout.getvalue().strip()
        report, offset = json.JSONDecoder().raw_decode(report_payload)
        self.assertEqual(report_payload[offset:].strip(), "")
        self.assertTrue(report_payload.startswith("{"))
        self.assertTrue(report_payload.endswith("}"))
        self.assertEqual(report_payload.count("{"), report_payload.count("}"))
        report_stored = json.loads((continued / "run.json").read_text())
        self.assertEqual(report, json.loads((continued / "run.json").read_text()))
        self.assertEqual([run["status"] for run in report["runs"]], ["failed", "completed"])
        self.assertEqual(report, report_stored)
        self.assertIn("failed 2 benchmark profiles: {}".format(continued), stderr.getvalue())
        self.assertIn("succeeds/summary.csv", stderr.getvalue())

        report_json_output = self.root / "continued-path-report.json"
        continued_path = self.root / "continued-path"
        path_stdout, path_stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(path_stdout), redirect_stderr(path_stderr):
            self.assertEqual(main(["run", "--config", str(config), "--output", str(continued_path), "--continue-on-error", "--report-json", str(report_json_output)], loader), 1)
        self.assertEqual(path_stdout.getvalue(), "")
        report = json.loads(report_json_output.read_text())
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["runs"], report_stored["runs"])
        self.assertIn("failed 2 benchmark profiles:", path_stderr.getvalue())
        self.assertIn("succeeds/summary.csv", path_stderr.getvalue())

    def test_registry_accepts_a_fake_adapter_and_generates_its_schema(self):
        """Adapters can be registered independently of the CLI, config loader, and executor."""
        registry = BenchmarkRegistry()
        fake = BenchmarkDefinition(
            name="fake-bench",
            description="test adapter",
            test_filter="Fake::Run",
            parameter_name="samples",
            parameter_description="Sample counts",
            parameter_environment="FAKE_SAMPLES",
            parameter_column="samples",
            parse_metrics=PING_BENCHMARK.parse_metrics,
            render_metrics=PING_BENCHMARK.render_metrics,
            validate_metrics=PING_BENCHMARK.validate_metrics,
            summarize_metrics=PING_BENCHMARK.summarize_metrics,
            render_summary=PING_BENCHMARK.render_summary,
        )
        self.assertIs(registry.register(fake), fake)
        self.assertEqual(list(registry), ["fake-bench"])
        schema = config_schema(registry)
        self.assertEqual(set(schema["properties"]), {"fake-bench"})
        self.assertIn("samples", schema["properties"]["fake-bench"]["additionalProperties"]["properties"])
        script = self._script(
            """
            echo "threads,actorPairs,samples,msgs_per_sec,elapsed_seconds,min_pair_sent_msgs,max_pair_sent_msgs"
            echo "1,32,1,1000,1.0,900,1100"
            echo "2,32,1,2000,1.0,1800,2200"
            """
        )
        output = self.root / "fake-output"
        output.mkdir()
        configuration = RunConfiguration(
            benchmark=fake,
            profile="fake",
            threads=(1, 2),
            actor_pairs=(32,),
            parameter_values=(1,),
            duration_seconds=1,
            repetitions=1,
            timeout_seconds=5,
        )
        manifest = run_actors_core(self._binary(script), configuration, output, {"commit_id": "test"})
        self.assertEqual(manifest["benchmark"], "fake-bench")
        self.assertIn("samples", (output / "summary.csv").read_text().splitlines()[0])

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
                affinity: [pack-numa-pack-chiplet]
            star-ping-bench:
              star-sweep:
                threads: [8]
                stars: [1, 2, 4]
                duration: 4
                repetitions: 2
                affinity: [none, spread-numa-pack-chiplet]
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

    def test_run_plan_expands_config_in_stable_queue_order(self):
        loaded = load_config(self._config(
            """
            ping-bench:
              first: {threads: [1], duration: 1, repetitions: 2, affinity: [none, pack-numa-pack-chiplet]}
              second: {threads: [1], duration: 1, repetitions: 1, affinity: [none]}
            """
        ))
        plan = build_run_plan(loaded)
        self.assertEqual([(s.profile, s.affinity, s.repeat) for s in plan.steps], [
            ("first", "none", 1), ("first", "none", 2),
            ("first", "pack-numa-pack-chiplet", 1), ("first", "pack-numa-pack-chiplet", 2),
            ("second", "none", 1),
        ])
        self.assertEqual(len({step.id for step in plan.steps}), len(plan.steps))

    def test_result_state_machine_rejects_invalid_transition_and_old_schema(self):
        pending = {"id": "step-1", "state": "pending", "artifacts": []}
        running = transition(pending, "running")
        self.assertEqual(transition(running, "passed")["state"], "passed")
        with self.assertRaisesRegex(BenchmarkError, "invalid result state transition"):
            transition(pending, "passed")
        old = self.root / "old.json"
        old.write_text('{"schema_version": 3}', encoding="utf-8")
        with self.assertRaisesRegex(BenchmarkError, "unsupported result manifest schema"):
            load_manifest(old)

    def test_result_store_never_publishes_missing_artifacts(self):
        path = self.root / "run.json"
        store = ResultStore(path, {"steps": [{"id": "step-1", "state": "pending", "artifacts": []}]})
        store.write()
        with self.assertRaisesRegex(BenchmarkError, "state pending"):
            store.add_artifacts("step-1", ["missing.txt"])
        store.transition_step("step-1", "running")
        with self.assertRaisesRegex(BenchmarkError, "not durably available"):
            store.add_artifacts("step-1", ["missing.txt"])
        artifact = self.root / "stdout.txt"
        artifact.write_text("ok", encoding="utf-8")
        store.add_artifacts("step-1", ["stdout.txt"])
        store.transition_step("step-1", "passed")
        self.assertEqual(load_manifest(path)["steps"][0]["artifacts"], ["stdout.txt"])
        with self.assertRaisesRegex(BenchmarkError, "state passed"):
            store.add_artifacts("step-1", ["stdout.txt"])

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
        with redirect_stderr(console):
            code = main(
                ["run", "--config", str(config), "--output", str(output)],
                resource_loader=lambda _: benchmark.read_bytes(),
                tool_revision={"build_type": "relwithdebinfo", "commit_id": "test"},
            )
        self.assertEqual(code, 0)
        manifest = json.loads((output / "run.json").read_text())
        self.assertEqual(manifest["status"], "completed")
        self.assertEqual(manifest["state"], "passed")
        self.assertEqual(manifest["schema_version"], SCHEMA_VERSION)
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
        self.assertEqual(generic_manifest["state"], "failed")
        self.assertEqual(generic_manifest["schema_version"], SCHEMA_VERSION)
        self.assertEqual(interrupted_manifest["status"], "interrupted")
        self.assertEqual(interrupted_manifest["state"], "cancelled")
        self.assertEqual(interrupted_manifest["schema_version"], SCHEMA_VERSION)

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
        self.assertEqual(stored["schema_version"], SCHEMA_VERSION)
        self.assertEqual(stored["state"], "passed")
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
        self.assertEqual(manifest["state"], "failed")
        self.assertEqual(manifest["schema_version"], SCHEMA_VERSION)

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
        self.assertEqual(manifest["state"], "failed")
        self.assertEqual(manifest["schema_version"], SCHEMA_VERSION)
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
        record = topology_record(topology)
        self.assertEqual(record["version"], 2)
        self.assertEqual(record["numa_nodes"], [
            {"id": 0, "cpus": [1, 2, 3]}, {"id": 1, "cpus": [4]},
        ])
        self.assertEqual(record["chiplets"], [
            {"numa_node": 0, "cpus": [1]}, {"numa_node": 0, "cpus": [2, 3]},
        ])

    def test_topology_hierarchy_from_synthetic_sysfs(self):
        cases = (
            {
                "name": "single_numa_smt",
                "nodes": ((0, "0-3"),),
                "l3": ((0, "0-3"),),
                "cpu_data": ((0, 0, 0, "0-1"), (1, 0, 0, "0-1"),
                             (2, 0, 1, "2-3"), (3, 0, 1, "2-3")),
                "allowed": (0, 1, 2, 3),
                "cores": ((0, 1), (2, 3)), "siblings": ((0, 1), (2, 3)), "reasons": (),
            },
            {
                "name": "multi_numa_multiple_chiplets_smt_off",
                "nodes": ((0, "0-1"), (1, "2-3")),
                "l3": ((0, "0-1"), (1, "2-3")),
                "cpu_data": ((0, 0, 0, "0"), (1, 0, 1, "1"),
                             (2, 1, 0, "2"), (3, 1, 1, "3")),
                "allowed": (0, 1, 2, 3),
                "cores": ((0,), (1,), (2,), (3,)), "siblings": ((0,), (1,), (2,), (3,)), "reasons": (),
            },
            {
                "name": "asymmetric_cpuset",
                "nodes": ((0, "0-3"),), "l3": ((0, "0-1"), (2, "2-3")),
                "cpu_data": ((0, 0, 0, "0-1"), (1, 0, 0, "0-1"),
                             (2, 0, 1, "2-3"), (3, 0, 1, "2-3")),
                "allowed": (1, 2), "cores": ((1,), (2,)), "siblings": ((1,), (2,)), "reasons": (),
            },
            {
                "name": "missing_numa_l3_and_incomplete_topology",
                "nodes": (), "l3": (),
                "cpu_data": ((0, None, None, None), (1, 0, 1, None)),
                "allowed": (0, 1), "cores": ((0,), (1,)), "siblings": ((0,), (1,)),
                "reasons": ("numa", "chiplet", "chiplet", "physical_core", "smt"),
            },
        )
        for case in cases:
            with self.subTest(case["name"]):
                sys_root = self.root / case["name"] / "sys" / "devices" / "system"
                for node_id, cpus in case["nodes"]:
                    node = sys_root / "node" / "node{}".format(node_id)
                    node.mkdir(parents=True)
                    node.joinpath("cpulist").write_text(cpus, encoding="utf-8")
                for cpu, cpus in case["l3"]:
                    cache = sys_root / "cpu" / "cpu{}".format(cpu) / "cache" / "index3"
                    cache.mkdir(parents=True)
                    cache.joinpath("level").write_text("3", encoding="utf-8")
                    cache.joinpath("shared_cpu_list").write_text(cpus, encoding="utf-8")
                for cpu, package, core, siblings in case["cpu_data"]:
                    topology = sys_root / "cpu" / "cpu{}".format(cpu) / "topology"
                    topology.mkdir(parents=True, exist_ok=True)
                    if package is not None:
                        topology.joinpath("physical_package_id").write_text(str(package), encoding="utf-8")
                    if core is not None:
                        topology.joinpath("core_id").write_text(str(core), encoding="utf-8")
                    if siblings is not None:
                        topology.joinpath("thread_siblings_list").write_text(siblings, encoding="utf-8")

                topology = discover_topology(sys_root, allowed_cpus=case["allowed"])
                self.assertEqual(topology.version, 2)
                self.assertEqual(topology.physical_cores, case["cores"])
                self.assertEqual(topology.smt_siblings, case["siblings"])
                self.assertEqual(tuple(level for level, _ in topology.hierarchy_reasons), case["reasons"])
                record = topology_record(topology)
                self.assertEqual(record["version"], 2)
                self.assertEqual(record["allowed_cpus"], list(case["allowed"]))

    def test_affinity_modes_select_compositional_deterministic_masks(self):
        topology = CpuTopology(
            allowed_cpus=tuple(range(16)),
            numa_nodes=((0, tuple(range(8))), (1, tuple(range(8, 16)))),
            chiplets=((0, (0, 1, 2, 3)), (0, (4, 5, 6, 7)),
                      (1, (8, 9, 10, 11)), (1, (12, 13, 14, 15))),
            physical_cores=((0, 1), (2, 3), (4, 5), (6, 7),
                            (8, 9), (10, 11), (12, 13), (14, 15)),
        )
        expected = {
            "none": None,
            "pack-numa": tuple(range(8)),
            "pack-numa-pack-chiplet": (0, 1, 2, 3),
            "spread-numa-pack-chiplet": (0, 1, 2, 3),
            "pack-numa-pack-chiplet-pack-core": (0, 1, 2, 3),
            "pack-numa-pack-chiplet-spread-core": (0, 1, 2),
            "pack-numa-spread-chiplet-pack-core": (0, 1, 4, 5),
            "pack-numa-spread-chiplet-spread-core": (0, 2, 4),
            "spread-numa-pack-chiplet-pack-core": (0, 1, 8, 9),
            "spread-numa-pack-chiplet-spread-core": (0, 2, 8),
            "spread-numa-spread-chiplet-pack-core": (0, 1, 8, 9),
            "spread-numa-spread-chiplet-spread-core": (0, 4, 8),
        }
        with mock.patch.object(os, "sched_setaffinity", create=True):
            for mode, cpus in expected.items():
                with self.subTest(mode=mode):
                    placement = plan_affinity(mode, topology, 3)
                    self.assertEqual(placement.cpus, cpus)
                    self.assertEqual(plan_affinity(mode, topology, 3), placement)

    def test_affinity_modes_support_smt_off_and_asymmetric_cpuset(self):
        topology = CpuTopology(
            allowed_cpus=(1, 2, 4, 7),
            numa_nodes=((0, (1, 2, 4, 7)),),
            chiplets=((0, (1, 2)), (0, (4, 7))),
            physical_cores=((1,), (2,), (4,), (7,)),
        )
        with mock.patch.object(os, "sched_setaffinity", create=True):
            self.assertEqual(plan_affinity("pack-numa", topology, 2).cpus, (1, 2, 4, 7))
            self.assertEqual(plan_affinity("pack-numa-spread-chiplet-spread-core", topology, 3).cpus,
                             (1, 2, 4))
            unsupported = plan_affinity("spread-numa-pack-chiplet", topology, 2)
        self.assertFalse(unsupported.supported)
        self.assertIn("spread-numa", unsupported.reason)

    def test_all_affinity_modes_with_smt_disabled(self):
        topology = CpuTopology(
            allowed_cpus=tuple(range(16)),
            numa_nodes=((0, tuple(range(8))), (1, tuple(range(8, 16)))),
            chiplets=((0, (0, 1, 2, 3)), (0, (4, 5, 6, 7)),
                      (1, (8, 9, 10, 11)), (1, (12, 13, 14, 15))),
            physical_cores=tuple((cpu,) for cpu in range(16)),
        )
        expected = {
            "none": None,
            "pack-numa": tuple(range(8)),
            "pack-numa-pack-chiplet": (0, 1, 2, 3),
            "spread-numa-pack-chiplet": (0, 1, 2, 3),
            "pack-numa-pack-chiplet-pack-core": (0, 1, 2),
            "pack-numa-pack-chiplet-spread-core": (0, 1, 2),
            "pack-numa-spread-chiplet-pack-core": (0, 2, 4),
            "pack-numa-spread-chiplet-spread-core": (0, 2, 4),
            "spread-numa-pack-chiplet-pack-core": (0, 2, 8),
            "spread-numa-pack-chiplet-spread-core": (0, 2, 8),
            "spread-numa-spread-chiplet-pack-core": (0, 4, 8),
            "spread-numa-spread-chiplet-spread-core": (0, 4, 8),
        }
        with mock.patch.object(os, "sched_setaffinity", create=True):
            for mode, cpus in expected.items():
                with self.subTest(mode=mode):
                    self.assertEqual(plan_affinity(mode, topology, 3).cpus, cpus)

    def test_unavailable_affinity_mode_is_reported_not_guessed(self):
        topology = CpuTopology(
            allowed_cpus=(0, 1),
            numa_nodes=((0, (0, 1)),),
            chiplets=((0, (0, 1)),),
        )
        with mock.patch.object(os, "sched_setaffinity", create=True):
            placement = plan_affinity("spread-numa-pack-chiplet", topology, 2)
        self.assertFalse(placement.supported)
        self.assertIn("spread-numa", placement.reason)

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
            affinity_modes=("spread-numa-pack-chiplet",),
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
        self.assertEqual(manifest["state"], "failed")
        self.assertIn("finished_at", manifest)
        self.assertIn("spread-numa-pack-chiplet", manifest["error"])
        self.assertEqual(manifest["runs"], [])
        self.assertEqual(manifest["affinity"][0]["status"], "unsupported")
        self.assertFalse((output / "summary.csv").exists())


class WebTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory(prefix="ydb-bench-web-test-")
        self.root = Path(self.temporary_directory.name)

    def tearDown(self):
        self.temporary_directory.cleanup()

    def _manifest(self, directory, status="completed", imported=False):
        directory.mkdir(parents=True, exist_ok=True)
        value = {"schema_version": SCHEMA_VERSION, "status": status, "state": "running" if status == "running" else "passed",
                 "started_at": "2025-01-01T00:00:00+00:00", "runs": [{"benchmark": "ping-bench", "profile": "baseline", "status": status}],
                 "steps": [{"id": "step-1", "state": "running" if status == "running" else "passed", "artifacts": []}],
                 "topology": {"version": 2, "allowed_cpus": [0], "numa_nodes": [{"id": 0, "cpus": [0]}]}}
        if imported:
            value["imported"] = True
        (directory / "run.json").write_text(json.dumps(value), encoding="utf-8")

    def _portable_archive(self, extra=None, version=SCHEMA_VERSION, corrupt=False):
        run = {"schema_version": version, "status": "completed", "state": "passed", "runs": [], "steps": [], "topology": {"version": 2}}
        files = {"run.json": json.dumps(run).encode(), "artifact.txt": b"artifact"}
        entries = [{"path": name, "sha256": hashlib.sha256(data).hexdigest(), "size": len(data)} for name, data in files.items()]
        if corrupt:
            entries[0]["sha256"] = "0" * 64
        manifest = json.dumps({"format_version": 1, "files": entries}).encode()
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, "w") as archive:
            archive.writestr("import.json", manifest)
            for name, data in files.items():
                archive.writestr(name, data)
            for name, data in (extra or {}).items():
                archive.writestr(name, data)
        return stream.getvalue()

    def test_import_rejects_hostile_corrupt_and_old_archives(self):
        with self.assertRaisesRegex(BenchmarkError, "unsafe"):
            import_archive(self.root, self._portable_archive({"../escape": b"x"}))
        with self.assertRaisesRegex(BenchmarkError, "hash mismatch"):
            import_archive(self.root, self._portable_archive(corrupt=True))
        with self.assertRaisesRegex(BenchmarkError, "schema version"):
            import_archive(self.root, self._portable_archive(version=3))
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, "w") as archive:
            link = zipfile.ZipInfo("link")
            link.external_attr = (stat.S_IFLNK | 0o777) << 16
            archive.writestr(link, "run.json")
            archive.writestr("import.json", b'{"format_version":1,"files":[]}')
        with self.assertRaisesRegex(BenchmarkError, "unexpected member type"):
            import_archive(self.root, stream.getvalue())

    def test_import_installs_immutable_normalized_result(self):
        imported = import_archive(self.root, self._portable_archive())
        self.assertEqual(imported["source"], "imported")
        run = self.root / imported["id"]
        self.assertEqual(read_model(self.root)[imported["id"]]["source"], "imported")
        self.assertFalse((run / "run.json").stat().st_mode & stat.S_IWUSR)
        with self.assertRaises(FileExistsError):
            (run / "artifact.txt").open("x")

    def test_comparison_key_rules(self):
        model = {
            "one": {"steps": [{"benchmark": "ping", "profile": "p", "affinity": "none"}, {"benchmark": "ping", "profile": "q", "affinity": "pack"}], "runs": []},
            "two": {"steps": [{"benchmark": "ping", "profile": "p", "affinity": "none"}, {"benchmark": "ping", "profile": "q", "affinity": "none"}], "runs": []},
        }
        keys = comparison_keys(model, ["one", "two"])
        self.assertEqual(keys["benchmark_profile_affinity"], ["ping/p/none"])
        self.assertEqual(keys["benchmark_profile_one_affinity"], ["ping/p", "ping/q"])
        self.assertEqual(keys["within_run_benchmark_profile"]["one"], ["ping/p", "ping/q"])

    def test_web_read_model_covers_active_completed_and_imported_runs(self):
        self._manifest(self.root / "active", "running")
        self._manifest(self.root / "complete")
        self._manifest(self.root / "imported", imported=True)
        model = read_model(self.root)
        self.assertEqual(model["active"]["status"], "running")
        self.assertEqual(model["complete"]["status"], "completed")
        self.assertEqual(model["imported"]["source"], "imported")

    def test_web_static_api_is_csp_protected_and_read_only(self):
        self._manifest(self.root / "complete")
        server = make_server("127.0.0.1", 0, self.root)
        worker = threading.Thread(target=server.serve_forever)
        worker.start()
        try:
            base = "http://127.0.0.1:{}".format(server.server_port)
            with urllib.request.urlopen(base + "/") as response:
                self.assertIn("default-src 'self'", response.headers["Content-Security-Policy"])
                self.assertIn(b"app.js", response.read())
            with urllib.request.urlopen(base + "/api/runs") as response:
                self.assertEqual(json.loads(response.read())[0]["id"], "complete")
            request = urllib.request.Request(base + "/api/import", data=self._portable_archive(), method="POST")
            with urllib.request.urlopen(request) as response:
                self.assertEqual(json.loads(response.read())["source"], "imported")
            with self.assertRaisesRegex(Exception, "HTTP Error 400"):
                urllib.request.urlopen(urllib.request.Request(base + "/api/runs", method="POST"))
        finally:
            server.shutdown()
            worker.join()
            server.server_close()

    def test_web_rejects_remote_listener_without_opt_in(self):
        self._manifest(self.root / "complete")
        with self.assertRaisesRegex(BenchmarkError, "allow-remote"):
            make_server("0.0.0.0", 0, self.root)
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            self.assertEqual(main(["web", "--listen", "0.0.0.0", "--output", str(self.root), "--no-open"]), 1)
        self.assertIn("allow-remote", stderr.getvalue())

    def test_web_run_api_validates_plans_runs_reconnects_and_cancels(self):
        """The web service owns a fake executor after each HTTP request ends."""
        started = threading.Event()
        release = threading.Event()

        def fake_executor(run, emit, cancelled):
            step = run["store"].manifest["steps"][0]
            emit({"type": "step-started", "step_id": step["id"]})
            emit({"type": "stdout", "data": "fake output\\n"})
            started.set()
            while not release.wait(.01):
                if cancelled.is_set():
                    return
            emit({"type": "step-finished", "step_id": step["id"], "state": "passed"})

        server = make_server("127.0.0.1", 0, self.root, executor=fake_executor)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        base = "http://127.0.0.1:{}".format(server.server_port)
        yaml_text = "ping-bench:\n  fake: {threads: [1], duration: 1, repetitions: 1, affinity: [none]}\n"
        try:
            def request(path, method="GET", body=None):
                value = urllib.request.urlopen(urllib.request.Request(base + path, data=body, method=method), timeout=3).read()
                return json.loads(value)
            self.assertTrue(request("/api/validate", "POST", yaml_text.encode())["valid"])
            self.assertEqual(len(request("/api/plan", "POST", yaml_text.encode())["plan"]), 1)
            created = request("/api/runs", "POST", yaml_text.encode())
            self.assertTrue(started.wait(2))
            detail = request("/api/runs/" + created["id"])
            self.assertEqual(detail["steps"][0]["state"], "running")
            self.assertIn("fake output", detail["tail"]["stdout"])
            self.assertIn("step-started", urllib.request.urlopen(base + "/api/runs/" + created["id"] + "/events").read().decode())
            self.assertTrue(request("/api/runs/" + created["id"] + "/cancel", "POST")["cancelled"])
            self.assertTrue(request("/api/runs/" + created["id"] + "/cancel", "POST")["cancelled"])
            for _ in range(100):
                if request("/api/runs/" + created["id"])["state"] == "cancelled":
                    break
                time.sleep(.01)
            self.assertEqual(request("/api/runs/" + created["id"])["state"], "cancelled")
        finally:
            release.set()
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
