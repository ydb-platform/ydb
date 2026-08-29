import hashlib
import inspect
import io
import json
import os
import plistlib
import shutil
import signal
import socket
import stat
import tempfile
import textwrap
import threading
import time
import unittest
import urllib.request
import zipfile
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import FrozenInstanceError, fields, replace
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote
from unittest import mock

import yaml

from ydb.tools.ydb_bench.lib import (
    actors_core,
    cli,
    common,
    import_results,
    linux_telemetry,
    load_control,
    local_ydb,
    runner,
    topology,
    web,
)
from ydb.tools.ydb_bench.lib.actors_core import (
    PING_BENCHMARK,
    STAR_PING_BENCHMARK,
    RunConfiguration,
    parse_metrics,
    run_actors_core,
)
from ydb.tools.ydb_bench.benchmarks import LOCAL_YDB_BENCHMARK, MEMORY_BENCHMARK
from ydb.tools.ydb_bench.benchmarks.local_ydb import parse_cli_metrics
from ydb.tools.ydb_bench.benchmarks.memory import parse_worker_metrics, validate_metrics as validate_memory_metrics
from ydb.tools.ydb_bench.benchmarks.registry import (
    BenchmarkDefinition,
    BenchmarkRegistry,
    DimensionDefinition,
    ParameterDefinition,
)
from ydb.tools.ydb_bench.lib.cli import main
from ydb.tools.ydb_bench.lib.common import BenchmarkError, BenchmarkInterrupted, extract_executable
from ydb.tools.ydb_bench.lib.config import CONFIG_SCHEMA, build_run_plan, config_schema, load_config
from ydb.tools.ydb_bench.lib.results import ResultStore, SCHEMA_VERSION, load_manifest, transition
from ydb.tools.ydb_bench.lib.runner import run_command
from ydb.tools.ydb_bench.lib.topology import (
    CpuTopology,
    _parse_darwin_topology,
    discover_topology,
    parse_cpu_list,
    plan_affinity,
    plan_background_load,
    topology_record,
)
from ydb.tools.ydb_bench.lib.import_results import export_archive, import_archive
from ydb.tools.ydb_bench.lib.web import (
    RunService,
    _add_memory_fairness_rows,
    chart_data,
    comparison_keys,
    make_server,
    read_model,
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

    def _worker_metrics_benchmark(self):
        return replace(
            PING_BENCHMARK,
            parse_worker_metrics=MEMORY_BENCHMARK.parse_worker_metrics,
            render_worker_metrics=MEMORY_BENCHMARK.render_worker_metrics,
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
        self.assertEqual(
            set(CONFIG_SCHEMA["properties"]),
            {"ping-bench", "star-ping-bench", "memory-bandwidth-bench", "local-ydb"},
        )
        local_load_schema = CONFIG_SCHEMA["properties"]["local-ydb"]["additionalProperties"]["properties"]["load"]
        self.assertEqual(local_load_schema["properties"]["allow-errors"], {"type": "boolean"})

    def test_local_ydb_cli_total_row_is_parsed_in_milliseconds(self):
        metrics = parse_cli_metrics("""
            Window Txs Txs/Sec Retries Errors p50(ms) p95(ms) p99(ms) pMax(ms)

            Total    Txs Txs/Sec Retries Errors p50(ms) p95(ms) p99(ms) pMax(ms)
                     9000 3000.5 4 2 1.2 2.5 4.0 9.5
            """)
        self.assertEqual(metrics["transactions"], 9000)
        self.assertEqual(metrics["throughput"], 3000.5)
        self.assertEqual(metrics["p99_ms"], 4.0)

    def test_local_ydb_cli_total_row_accepts_duration_column(self):
        metrics = parse_cli_metrics("""
            Total Txs Txs/Sec Retries Errors p50(ms) p95(ms) p99(ms) pMax(ms)
            2 20 10 0 0 137 317 419 419
            """)
        self.assertEqual(metrics["transactions"], 20)
        self.assertEqual(metrics["throughput"], 10)
        self.assertEqual(metrics["p99_ms"], 419)

    def test_local_ydb_cli_total_row_rejects_non_finite_metrics(self):
        with self.assertRaisesRegex(BenchmarkError, "valid Total row"):
            parse_cli_metrics("""
                Total Txs Txs/Sec Retries Errors p50(ms) p95(ms) p99(ms) pMax(ms)
                20 nan 0 0 1 2 3 4
            """)

    def test_local_ydb_profile_parses_geometry_load_and_role_affinity(self):
        loaded = load_config(self._config("""
                local-ydb:
                  storage-capacity:
                    workload:
                      type: kv
                      operation: upsert
                    geometry:
                      preset: storage
                      static-nodes: 2
                      dynamic-nodes: 2
                      max-dynamic-nodes: 6
                    client:
                      threads: 96
                    load:
                      parameter: rate
                      allow-errors: true
                      search:
                        start: 1000
                        maximum: 100000
                        multiplier: 2
                        resolution-percent: 5
                      objective:
                        type: latency-slo
                        percentile: p99
                        max-ms: 10
                        cpu-saturation-percent: 80
                    affinity:
                      ydb-cli:
                        mode: pack-numa-pack-chiplet-spread-core
                        cpus: one-chiplet
                      static-nodes:
                        mode: none
                      dynamic-nodes:
                        mode: none
                """))
        configuration = loaded.runs[0]
        self.assertIs(configuration.benchmark, LOCAL_YDB_BENCHMARK)
        self.assertEqual(configuration.affinity_modes, ("roles",))
        profile = configuration.parameters["local_ydb"]
        self.assertEqual(profile["geometry"]["static_nodes"], 2)
        self.assertEqual(profile["geometry"]["max_dynamic_nodes"], 6)
        self.assertEqual(profile["load"]["search"]["resolution_percent"], 5)
        self.assertEqual(profile["load"]["objective"]["max_ms"], 10)
        self.assertEqual(profile["load"]["objective"]["cpu_saturation_percent"], 80)
        self.assertTrue(profile["load"]["allow_errors"])
        self.assertEqual(profile["affinity"]["ydb_cli"]["cpus"], "one-chiplet")

    def test_local_ydb_allow_errors_defaults_to_false_and_requires_boolean(self):
        loaded = load_config(self._config("""
                local-ydb:
                  strict:
                    workload: {type: kv, operation: upsert}
                    load: {parameter: rate, values: [10]}
            """))
        self.assertFalse(loaded.runs[0].parameters["local_ydb"]["load"]["allow_errors"])

        invalid = self._config("""
            local-ydb:
              invalid:
                workload: {type: kv, operation: upsert}
                load: {parameter: rate, allow-errors: 1, values: [10]}
        """)
        with self.assertRaisesRegex(BenchmarkError, "allow-errors"):
            load_config(invalid)

    def test_local_ydb_kv_requires_at_least_two_columns(self):
        invalid = self._config("""
            local-ydb:
              invalid:
                workload:
                  type: kv
                  operation: upsert
                  options: {columns: 1}
                load: {parameter: rate, values: [10]}
        """)
        with self.assertRaisesRegex(BenchmarkError, "columns.*at least 2"):
            load_config(invalid)

    def test_local_ydb_legacy_load_controller_is_normalized(self):
        loaded = load_config(self._config("""
                local-ydb:
                  legacy:
                    workload: {type: kv, operation: upsert}
                    load:
                      mode: latency-slo
                      parameter: rate
                      start: 100
                      maximum: 1000
                      search-resolution-percent: 5
                      cpu-saturation-percent: 85
                      slo: {percentile: p95, max-ms: 20}
            """))
        load = loaded.runs[0].parameters["local_ydb"]["load"]
        self.assertNotIn("mode", load)
        self.assertEqual(load["search"]["resolution_percent"], 5)
        self.assertEqual(load["objective"]["type"], "latency-slo")
        self.assertEqual(load["objective"]["percentile"], "p95")
        self.assertEqual(load["objective"]["cpu_saturation_percent"], 85)

    def test_local_ydb_legacy_slo_requires_known_mapping_fields(self):
        invalid_slos = (
            "1",
            "{type: maximize-throughput, max-ms: 20}",
        )
        for index, slo in enumerate(invalid_slos):
            with self.subTest(slo=slo), self.assertRaisesRegex(BenchmarkError, r"load\.slo"):
                load_config(
                    self._config(
                        """
                        local-ydb:
                          invalid:
                            workload: {type: kv, operation: upsert}
                            load:
                              mode: latency-slo
                              parameter: rate
                              start: 10
                              maximum: 100
                              slo: __SLO__
                        """.replace("__SLO__", slo),
                        name="invalid-slo-{}.yaml".format(index),
                    )
                )

    def test_local_ydb_profile_is_editable_by_web_builder(self):
        loaded = load_config(self._config("""
            local-ydb:
              ui:
                workload:
                  type: stock
                  operation: put-rand-order
                geometry:
                  preset: storage
                  static-nodes: 2
                  dynamic-nodes: 2
                  max-dynamic-nodes: 4
                load:
                  parameter: rate
                  allow-errors: true
                  values: [10, 20]
                affinity:
                  ydb-cli:
                    mode: pack-numa-pack-chiplet-spread-core
                    cpus: one-chiplet
        """))
        model = web.editor_model(loaded, self.root / "results")
        benchmark = next(item for item in model["benchmarks"] if item["name"] == "local-ydb")
        profile = model["profiles"][0]
        self.assertTrue(benchmark["builder_supported"])
        self.assertEqual(benchmark["profile_kind"], "local-ydb")
        self.assertEqual(profile["local_ydb"]["workload"]["type"], "stock")
        self.assertEqual(profile["local_ydb"]["geometry"]["max_dynamic_nodes"], 4)
        self.assertEqual(profile["local_ydb"]["load"]["values"], [10, 20])
        self.assertTrue(profile["local_ydb"]["load"]["allow_errors"])
        self.assertEqual(profile["local_ydb"]["affinity"]["ydb_cli"]["cpus"], "one-chiplet")
        self.assertEqual(profile["parameters"], {})

    def test_local_ydb_profile_rejects_custom_geometry_without_dynamic_nodes(self):
        config = self._config("""
            local-ydb:
              invalid:
                workload: {type: kv, operation: select}
                geometry: {preset: custom}
                load: {mode: points, parameter: threads, values: [1]}
            """)
        with self.assertRaisesRegex(BenchmarkError, "custom preset requires dynamic-nodes"):
            load_config(config)

    def test_local_ydb_stock_profile(self):
        loaded = load_config(self._config("""
            local-ydb:
              stock-smoke:
                workload:
                  type: stock
                  operation: put-rand-order
                  options: {products: 10, orders: 0, min-partitions: 1, auto-partition: 0}
                load: {parameter: rate, values: [10]}
            """))
        workload = loaded.runs[0].parameters["local_ydb"]["workload"]
        self.assertEqual(workload["type"], "stock")
        self.assertEqual(workload["operation"], "put-rand-order")
        self.assertEqual(workload["options"]["products"], 10)

    def test_local_ydb_stock_commands_do_not_use_kv_path_option(self):
        cluster = mock.Mock(
            ydb_cli=Path("ydb"),
            client_endpoint="grpc://host.example:2135",
            database="/Root/bench",
        )
        command = local_ydb._stock_init_command(
            cluster,
            "ignored-table-prefix",
            {
                "products": 10,
                "quantity": 100,
                "orders": 0,
                "min-partitions": 1,
                "auto-partition": 0,
            },
        )
        self.assertNotIn("--path", command)
        self.assertNotIn("ignored-table-prefix", command)
        self.assertEqual(local_ydb._workload_table_path("stock", "ignored-table-prefix"), "stock")
        self.assertNotIn("--path", local_ydb._clean_workload_command(cluster, "stock", "ignored-table-prefix"))

        run_options = {"products": 10, "limit": 5}
        add_command = local_ydb._stock_run_command(
            cluster,
            "ignored-table-prefix",
            {"operation": "add-rand-order", "options": run_options},
            {"parameter": "threads"},
            8,
            30,
            64,
        )
        put_command = local_ydb._stock_run_command(
            cluster,
            "ignored-table-prefix",
            {"operation": "put-rand-order", "options": run_options},
            {"parameter": "threads"},
            8,
            30,
            64,
        )
        self.assertEqual(add_command[add_command.index("run") + 1], "add-rand-order")
        self.assertEqual(put_command[put_command.index("run") + 1], "put-rand-order")
        self.assertEqual(add_command[add_command.index("--threads") + 1], 8)

    def test_local_ydb_command_record_preserves_argv_and_affinity(self):
        result = runner.CommandResult(
            command=("/tmp/ydb cli", "workload", "stock", "run", "add-rand-order", "<unsafe>"),
            stdout="",
            stderr="",
            exit_code=0,
            started_at="2026-08-25T10:00:00+00:00",
            finished_at="2026-08-25T10:00:01+00:00",
            duration_seconds=1.0,
        )
        record = local_ydb._command_record("measuring", 2, result.command, (128, 0), result)
        self.assertEqual(record["argv"], list(result.command))
        self.assertEqual(record["cpu_affinity"], [0, 128])
        self.assertEqual(record["phase"], "measuring")
        self.assertEqual(record["repetition"], 2)
        self.assertEqual(record["exit_code"], 0)
        self.assertFalse(record["timed_out"])

    def test_local_ydb_search_records_commands_for_each_workload_phase(self):
        configuration = load_config(self._config("""
            local-ydb:
              audit:
                workload: {type: stock, operation: add-rand-order}
                load: {parameter: threads, values: [8]}
                measurement: {warmup: 1, duration: 1, repetitions: 1}
                affinity:
                  ydb-cli: {mode: none}
                  static-nodes: {mode: none}
                  dynamic-nodes: {mode: none}
        """)).runs[0]

        def command_result(command, stdout=""):
            return runner.CommandResult(
                command=tuple(str(part) for part in command),
                stdout=stdout,
                stderr="",
                exit_code=0,
                started_at="2026-08-25T10:00:00+00:00",
                finished_at="2026-08-25T10:00:01+00:00",
                duration_seconds=1.0,
            )

        cluster = mock.Mock(
            ydb_cli=Path("/tmp/ydb cli"),
            client_endpoint="grpc://benchmark-host:2135",
            database="/Root/bench",
            dynamic_nodes=[{}],
            static_pids=(10,),
            dynamic_pids=(20,),
        )
        cluster.init_workload.side_effect = lambda command: (
            command_result(command),
            [command_result(command)],
        )
        cluster._run.side_effect = lambda command, **_kwargs: command_result(command)
        monitor = mock.Mock(records=[])
        monitor.start.return_value = monitor
        monitor.stop.return_value = {
            "static_cpu_mean": 1,
            "static_cpu_max": 2,
            "dynamic_cpu_mean": 3,
            "dynamic_cpu_max": 4,
            "cli_cpu_mean": 5,
            "cli_cpu_max": 6,
            "host_cpu_mean": 7,
            "host_cpu_max": 8,
        }
        measurement_stdout = """
            Total Txs Txs/Sec Retries Errors p50(ms) p95(ms) p99(ms) pMax(ms)
            1 10 10 0 0 1 2 3 4
        """
        events = []
        output = self.root / "command-audit"
        binaries = {
            name: mock.Mock(path=self.root / name, sha256=name + "-digest", size=1)
            for name in ("ydbd", "ydb_cli", "process_guard")
        }
        with mock.patch.object(local_ydb, "LocalYdbCluster", return_value=cluster), mock.patch.object(
            local_ydb, "LinuxCpuMonitor", return_value=monitor
        ), mock.patch.object(
            local_ydb,
            "discover_topology",
            return_value=CpuTopology(
                allowed_cpus=(0,),
                numa_nodes=((0, (0,)),),
                chiplets=(),
                physical_cores=((0,),),
            ),
        ), mock.patch.object(
            local_ydb, "collect_system_info", return_value={}
        ), mock.patch.object(
            local_ydb,
            "run_command",
            side_effect=lambda command, *_args, **_kwargs: command_result(command, measurement_stdout),
        ):
            manifest = local_ydb.run_local_ydb(
                binaries,
                configuration,
                output,
                tool_revision="test",
                event_sink=events.append,
            )

        commands = manifest["attempts"][0]["commands"]
        self.assertEqual(manifest["timeout_seconds"], configuration.timeout_seconds)
        self.assertEqual(
            [command["phase"] for command in commands],
            ["initializing-workload", "warming-up", "measuring", "cleaning-workload"],
        )
        self.assertTrue(all(any(part in ("init", "run", "clean") for part in command["argv"]) for command in commands))
        measuring = next(command for command in commands if command["phase"] == "measuring")
        self.assertEqual(measuring["argv"][measuring["argv"].index("run") + 1], "add-rand-order")
        progress = [event["fields"]["progress"] for event in events if event["type"] == "step-progress"]
        running = [item for item in progress if item.get("current_command")]
        self.assertEqual([item["phase"] for item in running[:4]], [command["phase"] for command in commands])

    def test_local_ydb_kv_commands_keep_table_path(self):
        cluster = mock.Mock(
            ydb_cli=Path("ydb"),
            client_endpoint="grpc://host.example:2135",
            database="/Root/bench",
        )
        command = local_ydb._workload_base(cluster, "kv", "table-prefix")
        self.assertEqual(command[-2:], ["--path", "table-prefix"])
        self.assertEqual(local_ydb._workload_table_path("kv", "table-prefix"), "table-prefix")

    def test_load_controllers_find_capacity_and_latency_boundary(self):
        throughput_attempts = []
        throughput = load_control.search_load(
            {
                "parameter": "rate",
                "search": {"start": 10, "maximum": 80, "multiplier": 2, "resolution_percent": 5},
                "objective": {
                    "type": "maximize-throughput",
                    "target_role": "dynamic",
                    "cpu_saturation_percent": 90,
                    "plateau_gain_percent": 5,
                    "plateau_points": 1,
                },
            },
            lambda load: {
                "throughput": min(load, 40),
                "errors": 0,
                "dynamic_cpu_mean": min(100, load * 2.5),
                "static_cpu_mean": 10,
                "host_cpu_mean": 20,
            },
            on_attempt=lambda attempt: throughput_attempts.append(attempt["load"]),
        )
        self.assertEqual(throughput.selected_load, 38)
        self.assertEqual(throughput.outcome, "plateau-found")
        self.assertEqual(throughput_attempts[0], 10)
        self.assertEqual(len(throughput_attempts), len(set(throughput_attempts)))
        self.assertTrue(any(load not in (10, 20, 40, 80) for load in throughput_attempts))
        self.assertIn("plateau", throughput.stop_reason)

        latency_attempts = []
        latency = load_control.search_load(
            {
                "parameter": "rate",
                "search": {"start": 10, "maximum": 100, "multiplier": 2, "resolution_percent": 5},
                "objective": {
                    "type": "latency-slo",
                    "percentile": "p99",
                    "max_ms": 10,
                    "max_errors": 0,
                    "min_achieved_rate_ratio": 0.98,
                },
            },
            lambda load: {
                "throughput": load,
                "errors": 0,
                "p99_ms": 5 if load <= 60 else 15,
            },
            on_attempt=lambda attempt: latency_attempts.append(attempt["load"]),
        )
        self.assertGreaterEqual(latency.selected_load, 58)
        self.assertLessEqual(latency.selected_load, 60)
        self.assertEqual(latency.outcome, "boundary-found")
        self.assertEqual(latency.passing_load, latency.selected_load)
        self.assertGreater(latency.failing_load, latency.passing_load)
        self.assertEqual(len(latency_attempts), len(set(latency_attempts)))
        self.assertEqual(latency_attempts[:5], [10, 20, 40, 80, 60])

        below_start_attempts = []
        no_feasible_latency = load_control.search_load(
            {
                "parameter": "rate",
                "search": {"start": 10, "maximum": 100, "multiplier": 2, "resolution_percent": 5},
                "objective": {
                    "type": "latency-slo",
                    "percentile": "p99",
                    "max_ms": 10,
                    "max_errors": 0,
                    "min_achieved_rate_ratio": 0.98,
                },
            },
            lambda load: {"throughput": load, "errors": 0, "p99_ms": 15},
            on_attempt=lambda attempt: below_start_attempts.append(attempt["load"]),
        )
        self.assertIsNone(no_feasible_latency.selected_load)
        self.assertEqual(no_feasible_latency.outcome, "no-feasible-point")
        self.assertEqual(no_feasible_latency.failing_load, 10)
        self.assertEqual(below_start_attempts, [10])

    def test_throughput_plateau_uses_absolute_gain_and_stable_lowest_load(self):
        result = load_control.search_load(
            {
                "parameter": "rate",
                "search": {"start": 10, "maximum": 100, "multiplier": 2, "resolution_percent": 40},
                "objective": {
                    "type": "maximize-throughput",
                    "target_role": "dynamic",
                    "cpu_saturation_percent": 80,
                    "plateau_gain_percent": 2,
                    "plateau_points": 2,
                },
            },
            lambda load: {
                "throughput": 1000 - load,
                "errors": 0,
                "dynamic_cpu_mean": 90,
                "static_cpu_mean": 10,
                "host_cpu_mean": 20,
            },
        )
        self.assertEqual(result.outcome, "best-observed")
        self.assertEqual(result.selected_load, 10)
        self.assertEqual((result.attempts[0]["search_low"], result.attempts[0]["search_high"]), (10, 100))
        self.assertTrue(
            any(item["search_high"] - item["search_low"] < 90 for item in result.attempts if "search_low" in item)
        )

        selected = load_control._lowest_saturated_plateau_load(
            [
                {"load": 30, "throughput": 5600, "passed": True, "target_cpu_saturated": False},
                {"load": 54, "throughput": 5500, "passed": True, "target_cpu_saturated": True},
                {"load": 61, "throughput": 5637.45, "passed": True, "target_cpu_saturated": True},
                {"load": 340, "throughput": 5694.58, "passed": True, "target_cpu_saturated": True},
            ],
            2,
        )
        self.assertEqual(selected, 61)

    def test_throughput_controller_uses_ternary_search_and_error_bounds(self):
        config = {
            "parameter": "rate",
            "search": {"start": 10, "maximum": 100, "multiplier": 2, "resolution_percent": 2},
            "objective": {
                "type": "maximize-throughput",
                "target_role": "dynamic",
                "cpu_saturation_percent": 90,
                "plateau_gain_percent": 1,
                "plateau_points": 2,
            },
        }
        attempted = []
        result = load_control.search_load(
            config,
            lambda load: {
                "throughput": 100 - abs(load - 55),
                "errors": int(load >= 70),
                "dynamic_cpu_mean": 50,
                "static_cpu_mean": 10,
                "host_cpu_mean": 20,
            },
            on_attempt=lambda item: attempted.append(item["load"]),
        )
        self.assertLessEqual(abs(result.selected_load - 55), 2)
        self.assertEqual(result.outcome, "bounded-by-errors")
        self.assertEqual(result.failing_load, 70)
        self.assertEqual(len(attempted), len(set(attempted)))

        lower_bound = load_control.search_load(
            {**config, "search": {**config["search"], "maximum": 30}},
            lambda load: {
                "throughput": load,
                "errors": 0,
                "dynamic_cpu_mean": 50,
                "static_cpu_mean": 10,
                "host_cpu_mean": 20,
            },
        )
        self.assertEqual(lower_bound.selected_load, 30)
        self.assertEqual(lower_bound.outcome, "lower-bound")

        no_feasible = load_control.search_load(
            config,
            lambda load: {
                "throughput": load,
                "errors": 1,
                "dynamic_cpu_mean": 1,
                "static_cpu_mean": 1,
                "host_cpu_mean": 1,
            },
        )
        self.assertIsNone(no_feasible.selected_load)
        self.assertEqual(no_feasible.outcome, "no-feasible-point")
        self.assertEqual(no_feasible.failing_load, 10)

    def test_throughput_controller_keeps_zero_baseline_gain_json_safe(self):
        result = load_control.search_load(
            {
                "parameter": "rate",
                "search": {"start": 1, "maximum": 10, "multiplier": 2, "resolution_percent": 2},
                "objective": {
                    "type": "maximize-throughput",
                    "target_role": "dynamic",
                    "cpu_saturation_percent": 90,
                    "plateau_gain_percent": 1,
                    "plateau_points": 2,
                },
            },
            lambda load: {
                "throughput": 0 if load <= 4 else load,
                "errors": 0,
                "dynamic_cpu_mean": 20,
                "static_cpu_mean": 10,
                "host_cpu_mean": 30,
            },
        )
        zero_baseline_probe = next(item for item in result.attempts if item["load"] == 7)
        self.assertIsNone(zero_baseline_probe["throughput_gain_percent"])
        self.assertIn("zero baseline", zero_baseline_probe["decision"])
        json.dumps([dict(item) for item in result.attempts], allow_nan=False)

    def test_atomic_json_rejects_non_finite_numbers(self):
        path = self.root / "non-finite.json"
        with self.assertRaisesRegex(BenchmarkError, "finite values"):
            common.atomic_write_json(path, {"value": float("inf")})
        self.assertFalse(path.exists())

    def test_load_controllers_can_accept_reported_request_errors(self):
        strict = load_control.search_load(
            {"parameter": "rate", "values": [10]},
            lambda load: {"throughput": load, "errors": 1},
        )
        self.assertIsNone(strict.selected_load)
        self.assertEqual(strict.outcome, "no-feasible-point")

        points = load_control.search_load(
            {"parameter": "rate", "allow_errors": True, "values": [10, 20]},
            lambda load: {"throughput": load, "errors": load // 10},
        )
        self.assertEqual(points.selected_load, 20)
        self.assertTrue(all(item["passed"] for item in points.attempts))
        self.assertIn("errors allowed", points.attempts[-1]["decision"])

        throughput = load_control.search_load(
            {
                "parameter": "rate",
                "allow_errors": True,
                "search": {"start": 10, "maximum": 20, "multiplier": 2, "resolution_percent": 5},
                "objective": {
                    "type": "maximize-throughput",
                    "target_role": "dynamic",
                    "cpu_saturation_percent": 90,
                    "plateau_gain_percent": 1,
                    "plateau_points": 2,
                },
            },
            lambda load: {
                "throughput": load,
                "errors": 1,
                "dynamic_cpu_mean": 50,
                "static_cpu_mean": 10,
                "host_cpu_mean": 20,
            },
        )
        self.assertEqual(throughput.attempts[0]["load"], 10)
        self.assertEqual(len(throughput.attempts), len({item["load"] for item in throughput.attempts}))
        self.assertTrue(all(item["passed"] for item in throughput.attempts))
        self.assertEqual(throughput.selected_load, 20)

        latency = load_control.search_load(
            {
                "parameter": "rate",
                "allow_errors": True,
                "search": {"start": 10, "maximum": 20, "multiplier": 2, "resolution_percent": 5},
                "objective": {
                    "type": "latency-slo",
                    "percentile": "p99",
                    "max_ms": 10,
                    "max_errors": 0,
                    "min_achieved_rate_ratio": 0.98,
                },
            },
            lambda load: {"throughput": load, "errors": 100, "p99_ms": 5 if load == 10 else 15},
        )
        self.assertEqual(latency.selected_load, 10)
        self.assertGreater(latency.failing_load, latency.selected_load)
        self.assertTrue(latency.attempts[0]["passed"])
        self.assertIn("latency", latency.attempts[-1]["decision"])

    def test_linux_cpu_summary_weights_intervals_and_ignores_short_spikes_for_max(self):
        monitor = linux_telemetry.LinuxCpuMonitor({"dynamic": lambda: ()}, {"dynamic": 1}, interval=0.5)
        monitor._records = [
            {"elapsed_seconds": 0.5, "dynamic_cpu": 10.0, "host_cpu": 20.0},
            {"elapsed_seconds": 1.5, "dynamic_cpu": 30.0, "host_cpu": 40.0},
            {"elapsed_seconds": 0.01, "dynamic_cpu": 100.0, "host_cpu": 100.0},
        ]
        summary = monitor.summary()
        self.assertAlmostEqual(summary["dynamic_cpu_mean"], (5 + 45 + 1) / 2.01)
        self.assertEqual(summary["dynamic_cpu_max"], 30.0)
        self.assertAlmostEqual(summary["host_cpu_mean"], (10 + 60 + 1) / 2.01)
        self.assertEqual(summary["host_cpu_max"], 40.0)

    def test_local_ydb_attempt_aggregates_errors_across_repetitions(self):
        metrics = local_ydb._aggregate_measurements(
            [
                {"throughput": 10, "errors": 0},
                {"throughput": 20, "errors": 0},
                {"throughput": 30, "errors": 100},
            ]
        )
        self.assertEqual(metrics["throughput"], 20)
        self.assertEqual(metrics["errors"], 100)

    def test_local_ydb_scaling_uses_failing_boundary_and_minimum_attempt(self):
        attempts = (
            {"load": 50, "dynamic_cpu_mean": 90, "static_cpu_mean": 20},
            {"load": 100, "dynamic_cpu_mean": 100, "static_cpu_mean": 20},
        )
        boundary = load_control.LoadSearchResult(
            attempts,
            50,
            "latency SLO boundary",
            "boundary-found",
            passing_load=50,
            failing_load=100,
        )
        evidence, reason = local_ydb._search_scaling_evidence(boundary, 95)
        self.assertEqual(evidence["load"], 100)
        self.assertEqual(reason, "failing-boundary")

        minimum = load_control.LoadSearchResult(
            (attempts[-1],),
            None,
            "minimum load failed",
            "no-feasible-point",
            failing_load=100,
        )
        evidence, reason = local_ydb._search_scaling_evidence(minimum, 95)
        self.assertEqual(evidence["load"], 100)
        self.assertEqual(reason, "minimum-failing-load")

        dynamic_probe = load_control.LoadSearchResult(
            (
                {"load": 40, "throughput": 40, "dynamic_cpu_mean": 80, "static_cpu_mean": 20},
                {"load": 70, "throughput": 35, "dynamic_cpu_mean": 100, "static_cpu_mean": 20},
            ),
            40,
            "best observed point",
            "best-observed",
            passing_load=40,
        )
        evidence, reason = local_ydb._search_scaling_evidence(dynamic_probe, 95)
        self.assertEqual(evidence["load"], 70)
        self.assertEqual(reason, "dynamic-saturation")

        static_boundary = load_control.LoadSearchResult(
            (
                {"load": 50, "dynamic_cpu_mean": 100, "static_cpu_mean": 20},
                {"load": 100, "dynamic_cpu_mean": 100, "static_cpu_mean": 100},
            ),
            50,
            "static CPU boundary",
            "boundary-found",
            passing_load=50,
            failing_load=100,
        )
        evidence, reason = local_ydb._search_scaling_evidence(static_boundary, 95)
        self.assertEqual(evidence["load"], 100)
        self.assertEqual(reason, "failing-boundary")

    def test_local_ydb_stops_cpu_monitor_when_node_dies_before_measurement(self):
        configuration = load_config(self._config("""
            local-ydb:
              monitor-cleanup:
                workload: {type: kv, operation: upsert}
                load: {parameter: rate, values: [10]}
                measurement: {warmup: 0, duration: 1, repetitions: 1}
                affinity:
                  ydb-cli: {mode: none}
                  static-nodes: {mode: none}
                  dynamic-nodes: {mode: none}
        """)).runs[0]
        command_result = runner.CommandResult(
            command=("ydb", "workload", "kv", "init"),
            stdout="",
            stderr="",
            exit_code=0,
            started_at="2026-08-26T10:00:00+00:00",
            finished_at="2026-08-26T10:00:01+00:00",
            duration_seconds=1.0,
        )
        cluster = mock.Mock(
            ydb_cli=Path("ydb"),
            client_endpoint="grpc://benchmark-host:2135",
            database="/Root/bench",
            dynamic_nodes=[{}],
            static_pids=(10,),
            dynamic_pids=(20,),
        )
        cluster.init_workload.return_value = (command_result, [command_result])
        cluster._run.return_value = command_result
        cluster.ensure_running.side_effect = BenchmarkError("dynamic node exited")
        monitor = mock.Mock(records=[])
        monitor.stop.return_value = {}
        binaries = {
            name: mock.Mock(path=self.root / name, sha256=name + "-digest", size=1)
            for name in ("ydbd", "ydb_cli", "process_guard")
        }
        cpu_topology = CpuTopology(
            allowed_cpus=(0,),
            numa_nodes=((0, (0,)),),
            chiplets=(),
            physical_cores=((0,),),
        )
        with mock.patch.object(local_ydb, "LocalYdbCluster", return_value=cluster), mock.patch.object(
            local_ydb, "LinuxCpuMonitor", return_value=monitor
        ), mock.patch.object(local_ydb, "discover_topology", return_value=cpu_topology), mock.patch.object(
            local_ydb, "collect_system_info", return_value={}
        ):
            with self.assertRaisesRegex(BenchmarkError, "dynamic node exited"):
                local_ydb.run_local_ydb(
                    binaries,
                    configuration,
                    self.root / "monitor-cleanup",
                    tool_revision="test",
                )

        monitor.start.assert_called_once_with()
        monitor.stop.assert_called_once_with()
        cluster.stop.assert_called_once_with()

    def test_local_ydb_ctrl_c_marks_profile_cancelled(self):
        configuration = load_config(self._config("""
            local-ydb:
              interrupted-startup:
                workload: {type: kv, operation: upsert}
                load: {parameter: rate, values: [10]}
                affinity:
                  ydb-cli: {mode: none}
                  static-nodes: {mode: none}
                  dynamic-nodes: {mode: none}
        """)).runs[0]
        cluster = mock.Mock()
        cluster.start.side_effect = KeyboardInterrupt()
        binaries = {
            name: mock.Mock(path=self.root / name, sha256=name + "-digest", size=1)
            for name in ("ydbd", "ydb_cli", "process_guard")
        }
        cpu_topology = CpuTopology(
            allowed_cpus=(0,),
            numa_nodes=((0, (0,)),),
            chiplets=(),
            physical_cores=((0,),),
        )
        output = self.root / "interrupted-startup"
        with mock.patch.object(local_ydb, "LocalYdbCluster", return_value=cluster), mock.patch.object(
            local_ydb, "discover_topology", return_value=cpu_topology
        ), mock.patch.object(local_ydb, "collect_system_info", return_value={}):
            with self.assertRaisesRegex(BenchmarkInterrupted, "was interrupted"):
                local_ydb.run_local_ydb(binaries, configuration, output, tool_revision="test")

        manifest = json.loads((output / "run.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["status"], "interrupted")
        self.assertEqual(manifest["state"], "cancelled")
        self.assertEqual(manifest["progress"]["phase"], "cancelled")
        cluster.stop.assert_called_once_with()

    def test_role_masks_are_split_without_overlap(self):
        masks = local_ydb._split_mask((0, 1, 2, 3, 4, 5), 3)
        self.assertEqual(masks, ((0, 3), (1, 4), (2, 5)))
        self.assertEqual(set.intersection(*(set(mask) for mask in masks)), set())

    def test_local_ydb_role_affinity_is_validated_for_largest_geometry(self):
        geometry = {"static_nodes": 2, "max_dynamic_nodes": 4}
        affinities = {"static_nodes": (0, 1), "dynamic_nodes": (2, 3, 4)}
        with self.assertRaisesRegex(BenchmarkError, "3 explicitly assigned CPUs cannot host 4 nodes"):
            local_ydb._validate_role_affinity(geometry, affinities)

        local_ydb._validate_role_affinity(
            geometry,
            {"static_nodes": (0, 1), "dynamic_nodes": None},
        )

    def test_local_ydb_updates_affinity_for_every_process_thread(self):
        task_directory = self.root / "proc" / "101" / "task"
        for thread_id in (101, 102, 103):
            (task_directory / str(thread_id)).mkdir(parents=True)
        with mock.patch.object(local_ydb.os, "sched_setaffinity", create=True) as set_affinity:
            local_ydb._set_process_affinity(101, (4, 6), self.root / "proc")
        self.assertEqual(
            {call.args for call in set_affinity.call_args_list},
            {(101, (4, 6)), (102, (4, 6)), (103, (4, 6))},
        )

    def test_local_ydb_uses_mnc_port_ranges(self):
        candidates = local_ydb._mnc_port_candidates()
        with mock.patch.object(local_ydb, "_port_available", return_value=True):
            first = {name: local_ydb._next_available_port(ports, name) for name, ports in candidates.items()}
            second = {name: local_ydb._next_available_port(ports, name) for name, ports in candidates.items()}
        self.assertEqual(first, {"grpc_port": 2135, "ic_port": 19001, "mon_port": 8765})
        self.assertEqual(second, {"grpc_port": 20000, "ic_port": 19000, "mon_port": 31000})

    def test_local_ydb_skips_occupied_mnc_ports(self):
        with mock.patch.object(local_ydb, "_port_available", side_effect=(False, True)):
            port = local_ydb._next_available_port(iter((2135, 20000)), "grpc")
        self.assertEqual(port, 20000)

    def test_local_ydb_database_status_requires_running_state(self):
        status = """Database /Root/bench status:
  State: RUNNING
  Registered units:
    host:1234 - dynamic
    host:5678 - dynamic
  Data size hard quota: 0
"""
        self.assertEqual(local_ydb._registered_database_units(status), {"host:1234", "host:5678"})
        self.assertTrue(local_ydb._database_status_ready(status, {"host:1234", "host:5678"}))
        self.assertFalse(local_ydb._database_status_ready(status, {"host:1234", "host:9999"}))
        self.assertFalse(
            local_ydb._database_status_ready(
                status.replace("RUNNING", "PENDING_RESOURCES"),
                {"host:1234"},
            )
        )

    def test_local_ydb_static_nodes_use_self_management(self):
        config = local_ydb._cluster_config(
            (
                {"ic_port": 19001},
                {"ic_port": 19002},
            ),
            64,
            hostname="benchmark-host",
        )
        hosts = config["config"]["hosts"]
        host_configs = config["config"]["host_configs"]
        self.assertEqual([host["host"] for host in hosts], ["benchmark-host", "benchmark-host"])
        self.assertEqual(
            [host_config["ssd"] for host_config in host_configs],
            [["SectorMap:map_0:64:NONE"], ["SectorMap:map_1:64:NONE"]],
        )
        self.assertTrue(config["config"]["self_management_config"]["enabled"])
        self.assertNotIn("grpc_config", config["config"])

    def test_local_ydb_cluster_start_does_not_create_pdisk_files(self):
        cluster_directory = self.root / "sector-map-cluster"
        cluster = local_ydb.LocalYdbCluster(
            self.root / "ydbd",
            self.root / "ydb",
            self.root / "process_guard",
            cluster_directory,
            {"static_nodes": 1, "dynamic_nodes": 1, "disk_size_gb": 64},
            {"ydb_cli": None, "static_nodes": None, "dynamic_nodes": None},
            30,
        )
        ports = {"grpc_port": 2135, "ic_port": 19001, "mon_port": 8765}
        with mock.patch.object(cluster, "_node_ports", return_value=ports), mock.patch.object(
            cluster, "_wait_for_port"
        ), mock.patch.object(cluster, "_bootstrap_cluster"), mock.patch.object(
            cluster, "_create_tenant"
        ), mock.patch.object(
            cluster, "add_dynamic_nodes"
        ), mock.patch.object(
            local_ydb, "start_managed_process", return_value=mock.Mock(pid=1)
        ) as start_process:
            cluster.start()

        self.assertTrue((cluster_directory / "static-01").is_dir())
        self.assertFalse((cluster_directory / "static-01" / "pdisk.dat").exists())
        config = yaml.safe_load((cluster_directory / "cluster.yaml").read_text(encoding="utf-8"))
        self.assertEqual(config["config"]["host_configs"][0]["ssd"], ["SectorMap:map_0:64:NONE"])
        self.assertEqual(start_process.call_args.kwargs["parent_death_wrapper"], self.root / "process_guard")

    def test_local_ydb_scaling_waits_for_every_new_registered_node(self):
        cluster_directory = self.root / "scaling-cluster"
        cluster_directory.mkdir()
        cluster = local_ydb.LocalYdbCluster(
            self.root / "ydbd",
            self.root / "ydb",
            self.root / "process_guard",
            cluster_directory,
            {
                "static_nodes": 1,
                "dynamic_nodes": 1,
                "max_dynamic_nodes": 2,
                "storage_groups": 1,
                "disk_size_gb": 64,
            },
            {"ydb_cli": None, "static_nodes": None, "dynamic_nodes": None},
            30,
        )
        cluster.hostname = "benchmark-host"
        cluster.static_nodes = [{"grpc_port": 2135}]
        nodes = (
            {"grpc_port": 2136, "ic_port": 19002, "mon_port": 8766},
            {"grpc_port": 2137, "ic_port": 19003, "mon_port": 8767},
        )
        with mock.patch.object(cluster, "_node_ports", side_effect=nodes), mock.patch.object(
            cluster, "_wait_for_port"
        ) as wait_for_port, mock.patch.object(cluster, "_wait_database_ready") as wait_database, mock.patch.object(
            cluster, "_wait_tenant_ready"
        ), mock.patch.object(
            local_ydb, "start_managed_process", side_effect=(mock.Mock(pid=101), mock.Mock(pid=102))
        ):
            cluster.add_dynamic_nodes(2)

        self.assertEqual(
            wait_for_port.call_args_list,
            [mock.call(2136, "dynamic node 1"), mock.call(2137, "dynamic node 2")],
        )
        wait_database.assert_called_once_with({"benchmark-host:19002", "benchmark-host:19003"})

    def test_local_ydb_tenant_readiness_checks_every_dynamic_node(self):
        cluster_directory = self.root / "tenant-ready-cluster"
        cluster_directory.mkdir()
        cluster = local_ydb.LocalYdbCluster(
            self.root / "ydbd",
            self.root / "ydb",
            self.root / "process_guard",
            cluster_directory,
            {"static_nodes": 1, "dynamic_nodes": 1, "max_dynamic_nodes": 2, "disk_size_gb": 64},
            {"ydb_cli": None, "static_nodes": None, "dynamic_nodes": None},
            30,
        )
        cluster.hostname = "benchmark-host"
        cluster.dynamic_nodes = [{"grpc_port": 2136}, {"grpc_port": 2137}]
        channels = (mock.Mock(), mock.Mock())
        responses = (mock.Mock(Status=1), mock.Mock(Status=1))
        with mock.patch.object(
            local_ydb.grpc, "insecure_channel", side_effect=channels
        ) as open_channel, mock.patch.object(local_ydb.grpc_pb2_grpc, "TGRpcServerStub"), mock.patch.object(
            cluster,
            "_grpc_eventually",
            side_effect=((responses[0], [{"response": "first"}]), (responses[1], [{"response": "second"}])),
        ) as eventually:
            cluster._wait_tenant_ready(30)

        self.assertEqual(
            open_channel.call_args_list,
            [mock.call("benchmark-host:2136"), mock.call("benchmark-host:2137")],
        )
        self.assertEqual(
            [call.args[0] for call in eventually.call_args_list],
            ["tenant SchemeShard on dynamic node 1", "tenant SchemeShard on dynamic node 2"],
        )
        attempts = json.loads((cluster_directory / "tenant-ready-attempts.json").read_text(encoding="utf-8"))
        self.assertEqual([attempt["dynamic_node"] for attempt in attempts], [1, 2])
        self.assertTrue(all(channel.close.called for channel in channels))

    def test_local_ydb_init_rejects_partial_schema_after_cli_failure(self):
        cluster = local_ydb.LocalYdbCluster(
            self.root / "ydbd",
            self.root / "ydb",
            self.root / "process_guard",
            self.root / "partial-init-cluster",
            {"static_nodes": 1, "dynamic_nodes": 1, "disk_size_gb": 64},
            {"ydb_cli": None, "static_nodes": None, "dynamic_nodes": None},
            30,
        )
        cluster.static_processes = [mock.Mock(poll=mock.Mock(return_value=None))]
        cluster.dynamic_processes = [mock.Mock(poll=mock.Mock(return_value=None))]
        failed = runner.CommandResult(
            command=("ydb", "workload", "stock", "init"),
            stdout="tables were created before the failure",
            stderr="initial data load failed",
            exit_code=1,
            started_at="2026-08-25T10:00:00+00:00",
            finished_at="2026-08-25T10:00:01+00:00",
            duration_seconds=1.0,
        )
        describe_would_succeed = replace(failed, command=("ydb", "scheme", "describe"), exit_code=0)
        with mock.patch.object(local_ydb, "run_command", side_effect=(failed, describe_would_succeed)) as execute:
            with self.assertRaisesRegex(BenchmarkError, "initial data load failed"):
                cluster.init_workload(failed.command)
        self.assertEqual(execute.call_count, 1)

    def test_local_ydb_detects_exited_cluster_nodes(self):
        cluster = local_ydb.LocalYdbCluster(
            self.root / "ydbd",
            self.root / "ydb",
            self.root / "process_guard",
            self.root / "dead-node-cluster",
            {"static_nodes": 1, "dynamic_nodes": 1, "disk_size_gb": 64},
            {"ydb_cli": None, "static_nodes": None, "dynamic_nodes": None},
            30,
        )
        cluster.static_processes = [mock.Mock(poll=mock.Mock(return_value=None))]
        cluster.dynamic_processes = [mock.Mock(poll=mock.Mock(return_value=17))]
        with self.assertRaisesRegex(BenchmarkError, "dynamic node 1 exited with code 17"):
            cluster.ensure_running("measurement failed")

    def test_local_ydb_uses_mnc_bootstrap_and_tenant_requests(self):
        bootstrap = local_ydb._bootstrap_cluster_request()
        self.assertEqual(bootstrap.self_assembly_uuid, "multinode_cluster")
        self.assertEqual(bootstrap.operation_params.operation_mode, local_ydb.ydb_operation_pb2.OperationParams.SYNC)

        request = local_ydb._create_tenant_request("/Root/bench", "ssd", 3)
        self.assertEqual(request.path, "/Root/bench")
        self.assertEqual(request.idempotency_key, "ydb-bench-local-ydb")
        self.assertEqual(request.operation_params.operation_mode, local_ydb.ydb_operation_pb2.OperationParams.SYNC)
        self.assertEqual(len(request.resources.storage_units), 1)
        self.assertEqual(request.resources.storage_units[0].unit_kind, "ssd")
        self.assertEqual(request.resources.storage_units[0].count, 3)

        response = local_ydb.ydb_config_pb2.BootstrapClusterResponse()
        response.operation.ready = True
        response.operation.status = local_ydb.ydb_status_codes_pb2.StatusIds.SUCCESS
        self.assertTrue(local_ydb._operation_ready(response))
        local_ydb._require_successful_operation("bootstrap", response.operation)
        response.operation.status = local_ydb.ydb_status_codes_pb2.StatusIds.GENERIC_ERROR
        response.operation.issues.add(message="configuration rejected")
        with self.assertRaisesRegex(BenchmarkError, "GENERIC_ERROR: configuration rejected"):
            local_ydb._require_successful_operation("bootstrap", response.operation)

        with mock.patch.object(local_ydb.socket, "getfqdn", return_value="benchmark-host.example.net"):
            default_config = local_ydb._cluster_config(
                ({"ic_port": 19001},),
                64,
            )
        self.assertEqual(default_config["config"]["hosts"][0]["host"], "benchmark-host.example.net")

    def test_cli_json_discovery_and_validation_do_not_create_output(self):
        config = self._config("""
            ping-bench:
              invalid:
                threads: []
                duration: 1
                repetitions: 1
                affinity: [none]
            """)
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
        benchmark = self._script("""
            test "$ACTORSYSTEM_INFLIGHTS" = "2" || exit 23
            echo "threads,actorPairs,in_flight,msgs_per_sec,elapsed_seconds,min_pair_sent_msgs,max_pair_sent_msgs"
            echo "1,32,2,1000,1.0,900,1100"
            """)
        config = self._config("""
            ping-bench:
              fails: {threads: [1], actor-pairs: [32], inflight: [1], duration: 1, repetitions: 2, affinity: [none]}
              succeeds: {threads: [1], actor-pairs: [32], inflight: [2], duration: 1, repetitions: 1, affinity: [none]}
            """)

        def loader(_):
            return benchmark.read_bytes()

        fail_fast = self.root / "fail-fast"
        fail_fast_stdout, fail_fast_stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(fail_fast_stdout), redirect_stderr(fail_fast_stderr):
            self.assertEqual(main(["run", "--config", str(config), "--output", str(fail_fast)], loader), 1)
        fail_fast_manifest = json.loads((fail_fast / "run.json").read_text())
        self.assertEqual(len(fail_fast_manifest["runs"]), 1)
        self.assertEqual([step["state"] for step in fail_fast_manifest["steps"]], ["failed", "cancelled", "cancelled"])
        self.assertTrue(all(step.get("reason") for step in fail_fast_manifest["steps"] if step["state"] == "cancelled"))
        self.assertEqual(fail_fast_stdout.getvalue(), "")
        self.assertIn("failed 2 benchmark profiles: {}".format(fail_fast), fail_fast_stderr.getvalue())

        continued, stdout, stderr = self.root / "continued", io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            self.assertEqual(
                main(
                    [
                        "run",
                        "--config",
                        str(config),
                        "--output",
                        str(continued),
                        "--continue-on-error",
                        "--report-json",
                        "-",
                    ],
                    loader,
                ),
                1,
            )
        report_payload = stdout.getvalue().strip()
        report, offset = json.JSONDecoder().raw_decode(report_payload)
        self.assertEqual(report_payload[offset:].strip(), "")
        self.assertTrue(report_payload.startswith("{"))
        self.assertTrue(report_payload.endswith("}"))
        self.assertEqual(report_payload.count("{"), report_payload.count("}"))
        report_stored = json.loads((continued / "run.json").read_text())
        self.assertEqual(report, json.loads((continued / "run.json").read_text()))
        self.assertEqual([run["status"] for run in report["runs"]], ["failed", "completed"])
        self.assertEqual([step["state"] for step in report["steps"]], ["failed", "cancelled", "passed"])
        self.assertIn("profile stopped after failure", report["steps"][1]["reason"])
        self.assertEqual(report, report_stored)
        self.assertIn("failed 2 benchmark profiles: {}".format(continued), stderr.getvalue())
        self.assertIn("succeeds/summary.csv", stderr.getvalue())

        report_json_output = self.root / "continued-path-report.json"
        continued_path = self.root / "continued-path"
        path_stdout, path_stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(path_stdout), redirect_stderr(path_stderr):
            self.assertEqual(
                main(
                    [
                        "run",
                        "--config",
                        str(config),
                        "--output",
                        str(continued_path),
                        "--continue-on-error",
                        "--report-json",
                        str(report_json_output),
                    ],
                    loader,
                ),
                1,
            )
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
            resource_name="actors_core_ut_fat",
            parameters=(
                ParameterDefinition(
                    "actor-pairs", "pairs", default=(512,), environment="ACTORSYSTEM_ACTOR_PAIRS", column="actorPairs"
                ),
                ParameterDefinition(
                    "samples", "Sample counts", default=(1,), environment="FAKE_SAMPLES", column="samples"
                ),
            ),
            dimensions=(
                DimensionDefinition("threads"),
                DimensionDefinition("actorPairs"),
                DimensionDefinition("samples"),
            ),
            metrics=PING_BENCHMARK.metrics,
            parse_metrics=PING_BENCHMARK.parse_metrics,
            render_metrics=PING_BENCHMARK.render_metrics,
            validate_metrics=PING_BENCHMARK.validate_metrics,
            summarize_metrics=PING_BENCHMARK.summarize_metrics,
            render_summary=PING_BENCHMARK.render_summary,
            command=lambda binary, benchmark, configuration, case: [str(binary), "Fake::Run"],
            environment=lambda configuration, case: {
                "ACTORSYSTEM_THREADS": str(case["threads"]),
                "ACTORSYSTEM_ACTOR_PAIRS": ",".join(map(str, configuration.parameters["actor-pairs"])),
                "FAKE_SAMPLES": ",".join(map(str, configuration.parameters["samples"])),
            },
            process_cases=PING_BENCHMARK.process_cases,
        )
        self.assertIs(registry.register(fake), fake)
        self.assertEqual(list(registry), ["fake-bench"])
        schema = config_schema(registry)
        self.assertEqual(set(schema["properties"]), {"fake-bench"})
        self.assertIn("samples", schema["properties"]["fake-bench"]["additionalProperties"]["properties"])
        script = self._script("""
            echo "threads,actorPairs,samples,msgs_per_sec,elapsed_seconds,min_pair_sent_msgs,max_pair_sent_msgs"
            if test "$ACTORSYSTEM_THREADS" = "1"; then
              echo "1,32,1,1000,1.0,900,1100"
            else
              echo "2,32,1,2000,1.0,1800,2200"
            fi
            """)
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

    def test_benchmark_definition_declares_immutable_test_filter(self):
        """The actor test selector is frozen benchmark metadata, not a post-construction attribute."""
        self.assertIn("test_filter", {field.name for field in fields(BenchmarkDefinition)})
        self.assertEqual(PING_BENCHMARK.test_filter, "HeavyActorBenchmark::SendActivateReceiveCSVManual")
        with self.assertRaises(FrozenInstanceError):
            PING_BENCHMARK.test_filter = "Other::Filter"

    def test_config_supports_multiple_benchmarks_and_profiles(self):
        """Load ping baseline, ping focused, then star sweep while preserving YAML order."""
        config = self._config("""
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
            """)
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
        loaded = load_config(self._config("""
            ping-bench:
              first: {threads: [1, 2], duration: 1, repetitions: 2, affinity: [none, pack-numa-pack-chiplet]}
              second: {threads: [1], duration: 1, repetitions: 1, affinity: [none]}
            """))
        plan = build_run_plan(loaded)
        self.assertEqual(
            [(s.profile, s.affinity, s.threads, s.repeat) for s in plan.steps],
            [
                ("first", "none", 1, 1),
                ("first", "none", 1, 2),
                ("first", "none", 2, 1),
                ("first", "none", 2, 2),
                ("first", "pack-numa-pack-chiplet", 1, 1),
                ("first", "pack-numa-pack-chiplet", 1, 2),
                ("first", "pack-numa-pack-chiplet", 2, 1),
                ("first", "pack-numa-pack-chiplet", 2, 2),
                ("second", "none", 1, 1),
            ],
        )
        self.assertEqual(len({step.id for step in plan.steps}), len(plan.steps))

    def test_cli_reuses_precomputed_step_index_and_rejects_unknown_events(self):
        """Event lookup never rescans the immutable plan and diagnoses keys absent from it."""

        class OnePassSteps:
            def __init__(self, values):
                self.values = values
                self.iterations = 0

            def __iter__(self):
                self.iterations += 1
                if self.iterations > 1:
                    raise AssertionError("plan steps were scanned again")
                return iter(self.values)

        config = self._config(
            """
            ping-bench:
              indexed: {threads: [1], actor-pairs: [32], inflight: [1], duration: 1, repetitions: 1, affinity: [none]}
            """,
            name="indexed.yaml",
        )

        def indexed_plan():
            plan = build_run_plan(load_config(config))
            steps = OnePassSteps(plan.steps)
            return replace(plan, steps=steps), steps

        def successful_run(_binary, _configuration, _output_directory, **kwargs):
            emit = kwargs["event_sink"]
            event = {"affinity": "none", "threads": 1, "case": 1, "repeat": 1}
            emit({"type": "step-started", **event})
            emit({"type": "step-finished", "state": "passed", **event})
            return {"summary": "summary.csv"}

        plan, steps = indexed_plan()
        with mock.patch.object(cli, "build_run_plan", return_value=plan), mock.patch.object(
            cli, "run_benchmark", side_effect=successful_run
        ):
            self.assertEqual(
                cli.main(
                    ["run", "--config", str(config), "--output", str(self.root / "indexed-output")],
                    resource_loader=lambda _: b"#!/bin/sh\nexit 0\n",
                ),
                0,
            )
        self.assertEqual(steps.iterations, 1)

        def unknown_run(_binary, _configuration, _output_directory, **kwargs):
            kwargs["event_sink"]({"type": "step-started", "affinity": "none", "threads": 99, "case": 1, "repeat": 1})

        plan, steps = indexed_plan()
        error = io.StringIO()
        with redirect_stderr(error), mock.patch.object(cli, "build_run_plan", return_value=plan), mock.patch.object(
            cli, "run_benchmark", side_effect=unknown_run
        ):
            self.assertEqual(
                cli.main(
                    ["run", "--config", str(config), "--output", str(self.root / "unknown-event-output")],
                    resource_loader=lambda _: b"#!/bin/sh\nexit 0\n",
                ),
                1,
            )
        self.assertEqual(steps.iterations, 1)
        self.assertIn("benchmark event does not match a planned step", error.getvalue())

    def test_memory_config_expands_generic_parameter_matrix(self):
        loaded = load_config(self._config("""
            memory-bandwidth-bench:
              mixed:
                threads: [1, 2]
                random-percent: [0, 50]
                random-mode: [copy, write]
                buffer-size-mb: [8]
                part-size-kb: [512]
                duration: 1
                repetitions: 2
                affinity: [none, pack-numa]
            """))
        configuration = loaded.runs[0]
        self.assertIs(configuration.benchmark, MEMORY_BENCHMARK)
        self.assertEqual(configuration.parameters["random-percent"], (0, 50))
        plan = build_run_plan(loaded)
        self.assertEqual(len(plan.steps), 32)
        self.assertEqual(plan.steps[0].parameters["random-percent"], 0)
        self.assertEqual(plan.steps[-1].parameters["random-mode"], "write")
        self.assertEqual({step.threads for step in plan.steps}, {1, 2})
        self.assertEqual({step.affinity for step in plan.steps}, {"none", "pack-numa"})

    def test_automatic_timeout_counts_measurements_inside_each_process(self):
        """Memory cases time one measurement; actor processes time their pairs/value matrix, never other threads."""
        actors = load_config(
            self._config(
                """
                ping-bench:
                  sweep:
                    threads: [1, 2, 4]
                    actor-pairs: [32, 64]
                    inflight: [1, 2, 4]
                    duration: 5
                    repetitions: 1
                    affinity: [none]
                """,
                name="actor-timeout.yaml",
            )
        ).runs[0]
        memory = load_config(
            self._config(
                """
                memory-bandwidth-bench:
                  sweep:
                    threads: [1, 2, 4]
                    random-percent: [0, 50, 100]
                    random-mode: [copy, write]
                    buffer-size-mb: [8, 16]
                    part-size-kb: [512, 1024]
                    duration: 5
                    repetitions: 1
                    affinity: [none]
                """,
                name="memory-timeout.yaml",
            )
        ).runs[0]

        with self.subTest(benchmark="actors"):
            self.assertEqual(actors.timeout_seconds, 2 * 3 * 5 * 3 + 30)
        with self.subTest(benchmark="memory"):
            self.assertEqual(memory.timeout_seconds, 5 * 3 + 30)

    def test_memory_metric_validation_requires_process_case(self):
        """The adapter contract exposes case as required and rejects old two-argument calls."""
        case_parameter = inspect.signature(validate_memory_metrics).parameters["case"]
        self.assertIs(case_parameter.default, inspect.Parameter.empty)
        with self.assertRaises(TypeError):
            validate_memory_metrics([], self._configuration(benchmark=MEMORY_BENCHMARK))

    def test_memory_worker_metrics_keep_raw_workers(self):
        stdout = "\n".join(
            (
                "workers.csv",
                "worker,scope,operations,payload_bytes,read_bytes,written_bytes,ops_per_sec,payload_mb_per_sec,read_mb_per_sec,write_mb_per_sec,memory_traffic_mb_per_sec",
                "0,sequential,10,20,20,20,100,200,200,200,400",
                "1,random,30,30,30,30,300,300,300,300,600",
            )
        )
        rows = parse_worker_metrics(stdout, MEMORY_BENCHMARK)
        self.assertEqual([(row["worker"], row["scope"]) for row in rows], [(0, "sequential"), (1, "random")])
        self.assertEqual(rows[1]["ops_per_sec"], 300.0)

    def test_missing_worker_metrics_finalizes_manifest_and_step(self):
        benchmark = self._worker_metrics_benchmark()
        script = self._script("""
            echo "threads,actorPairs,in_flight,msgs_per_sec,elapsed_seconds,min_pair_sent_msgs,max_pair_sent_msgs"
            echo "1,32,1,1000,1.0,900,1100"
            """)
        output = self.root / "missing-worker-output"
        output.mkdir()
        events = []

        with self.assertRaisesRegex(BenchmarkError, "does not contain workers.csv"):
            run_actors_core(
                self._binary(script),
                self._configuration(benchmark=benchmark),
                output,
                tool_revision={"commit_id": "test"},
                event_sink=events.append,
            )

        manifest = json.loads((output / "run.json").read_text())
        repetition = output / "none" / "threads-001" / "repeat-001"
        self.assertEqual(manifest["status"], "failed")
        self.assertEqual(manifest["state"], "failed")
        self.assertIn("does not contain workers.csv", manifest["error"])
        self.assertNotIn("metrics", manifest["runs"][0])
        self.assertNotIn("worker_metrics", manifest["runs"][0])
        self.assertFalse((repetition / "metrics.csv").exists())
        self.assertFalse((repetition / "workers.csv").exists())
        self.assertEqual([event["type"] for event in events], ["step-started", "step-finished"])
        self.assertEqual(events[-1]["state"], "failed")
        self.assertEqual(events[-1]["fields"]["error"], manifest["error"])

    def test_empty_worker_metrics_finalizes_manifest_and_step(self):
        benchmark = self._worker_metrics_benchmark()
        script = self._script("""
            echo "threads,actorPairs,in_flight,msgs_per_sec,elapsed_seconds,min_pair_sent_msgs,max_pair_sent_msgs"
            echo "1,32,1,1000,1.0,900,1100"
            echo "workers.csv"
            echo "worker,scope,operations"
            """)
        output = self.root / "empty-worker-output"
        output.mkdir()
        events = []

        with self.assertRaisesRegex(BenchmarkError, "produced no worker metrics"):
            run_actors_core(
                self._binary(script),
                self._configuration(benchmark=benchmark),
                output,
                tool_revision={"commit_id": "test"},
                event_sink=events.append,
            )

        manifest = json.loads((output / "run.json").read_text())
        self.assertEqual(manifest["status"], "failed")
        self.assertEqual(manifest["state"], "failed")
        self.assertIn("produced no worker metrics", manifest["runs"][0]["error"])
        self.assertEqual(events[-1]["type"], "step-finished")
        self.assertEqual(events[-1]["state"], "failed")
        self.assertEqual(events[-1]["fields"]["error"], manifest["error"])

    def test_worker_metrics_write_failure_rolls_back_metric_artifacts(self):
        benchmark = self._worker_metrics_benchmark()
        script = self._script("""
            echo "threads,actorPairs,in_flight,msgs_per_sec,elapsed_seconds,min_pair_sent_msgs,max_pair_sent_msgs"
            echo "1,32,1,1000,1.0,900,1100"
            echo "workers.csv"
            echo "worker,scope,operations"
            echo "0,sequential,10"
            """)
        output = self.root / "worker-write-failure-output"
        output.mkdir()
        events = []

        original_atomic_write_text = actors_core.atomic_write_text

        def write_or_fail(path, contents):
            if Path(path).name == "workers.csv":
                raise OSError("worker metrics disk full")
            return original_atomic_write_text(path, contents)

        with mock.patch.object(actors_core, "atomic_write_text", side_effect=write_or_fail):
            with self.assertRaisesRegex(BenchmarkError, "worker metrics disk full"):
                run_actors_core(
                    self._binary(script),
                    self._configuration(benchmark=benchmark),
                    output,
                    tool_revision={"commit_id": "test"},
                    event_sink=events.append,
                )

        manifest = json.loads((output / "run.json").read_text())
        repetition = output / "none" / "threads-001" / "repeat-001"
        self.assertEqual(manifest["status"], "failed")
        self.assertEqual(manifest["state"], "failed")
        self.assertEqual(manifest["error"], "worker metrics disk full")
        self.assertNotIn("metrics", manifest["runs"][0])
        self.assertNotIn("worker_metrics", manifest["runs"][0])
        self.assertFalse((repetition / "metrics.csv").exists())
        self.assertFalse((repetition / "workers.csv").exists())
        self.assertEqual(events[-1]["type"], "step-finished")
        self.assertEqual(events[-1]["state"], "failed")
        self.assertEqual(events[-1]["fields"]["error"], manifest["error"])

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
        old.write_text("[]", encoding="utf-8")
        with self.assertRaisesRegex(BenchmarkError, "JSON object"):
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

    def test_result_store_updates_progress_only_for_running_steps(self):
        path = self.root / "progress-run.json"
        store = ResultStore(path, {"steps": [{"id": "step-1", "state": "pending", "artifacts": []}]})
        store.write()
        with self.assertRaisesRegex(BenchmarkError, "state pending"):
            store.update_step("step-1", progress={"phase": "preparing"})
        store.transition_step("step-1", "running")
        store.update_step("step-1", progress={"phase": "measuring", "attempt": 3})
        self.assertEqual(load_manifest(path)["steps"][0]["progress"]["attempt"], 3)
        store.transition_step("step-1", "passed")
        with self.assertRaisesRegex(BenchmarkError, "state passed"):
            store.update_step("step-1", progress={"phase": "completed"})

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
        config = self._config("""
            ping-bench:
              baseline:
                threads: [1]
                threads: [2]
                duration: 1
                repetitions: 1
                affinity: [none]
            """)
        with self.assertRaisesRegex(BenchmarkError, "duplicate key 'threads'"):
            load_config(config)

    def test_perf_requires_profile_build(self):
        config = self._config("""
            ping-bench:
              baseline:
                threads: [1]
                duration: 1
                repetitions: 1
                affinity: [none]
            """)
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
        benchmark = self._script("""
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
            """)
        config = self._config("""
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
            """)
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

    def test_cli_generated_manifest_round_trips_through_portable_archive(self):
        """A real CLI manifest keeps its integer case index when imported."""
        benchmark = self._script("""
            test "$1" = "HeavyActorBenchmark::SendActivateReceiveCSVManual" || exit 10
            echo "threads,actorPairs,in_flight,msgs_per_sec,elapsed_seconds,min_pair_sent_msgs,max_pair_sent_msgs"
            echo "1,32,1,1000,1.0,900,1100"
            """)
        config = self._config("""
            ping-bench:
              portable:
                threads: [1]
                actor-pairs: [32]
                inflight: [1]
                duration: 1
                repetitions: 1
                affinity: [none]
            """)
        output = self.root / "portable-source"
        with redirect_stderr(io.StringIO()):
            self.assertEqual(
                main(
                    ["run", "--config", str(config), "--output", str(output)],
                    resource_loader=lambda _: benchmark.read_bytes(),
                    tool_revision={"build_type": "relwithdebinfo", "commit_id": "test"},
                ),
                0,
            )

        produced = load_manifest(output / "run.json")
        self.assertEqual(produced["steps"][0]["case"], 1)
        destination = self.root / "portable-destination"
        with export_archive(output) as archive:
            imported = import_archive(destination, archive.read_bytes())
        restored = load_manifest(destination / imported["id"] / "run.json")
        self.assertEqual(restored["steps"][0]["case"], 1)
        self.assertEqual(restored["steps"][0]["parameters"], produced["steps"][0]["parameters"])

    def test_cli_exit_code_uses_interruption_error_type(self):
        config = self._config("""
            ping-bench:
              test:
                threads: [1]
                duration: 1
                repetitions: 1
                affinity: [none]
            """)

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
        self.assertTrue(interrupted_manifest["steps"])
        self.assertTrue(all(step["state"] == "cancelled" for step in interrupted_manifest["steps"]))
        self.assertTrue(all("benchmark stopped" in step["reason"] for step in interrupted_manifest["steps"]))

    def test_run_writes_manifest_raw_metrics_and_median_summary(self):
        script = self._script("""
            test "$1" = "HeavyActorBenchmark::SendActivateReceiveCSVManual" || exit 10
            test "$ACTORSYSTEM_TEST_MODE" = "manual" || exit 11
            test "$ACTORSYSTEM_THREADS" = "1" -o "$ACTORSYSTEM_THREADS" = "2" || exit 12
            test "$ACTORSYSTEM_ACTOR_PAIRS" = "32" || exit 13
            test "$ACTORSYSTEM_INFLIGHTS" = "1" || exit 14
            test "$ACTORSYSTEM_DURATION" = "1" || exit 15
            echo "[ RUN      ] benchmark"
            echo "threads,actorPairs,in_flight,msgs_per_sec,elapsed_seconds,min_pair_sent_msgs,max_pair_sent_msgs"
            if test "$ACTORSYSTEM_THREADS" = "1"; then
              echo "1,32,1,1000,1.0,900,1100"
            else
              echo "2,32,1,2000,1.0,1800,2200"
            fi
            """)
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
        self.assertEqual(len(manifest["runs"]), 6)
        self.assertTrue((output / "summary.csv").is_file())
        self.assertIn("none,1,32,1,3,1000.0,1000.0,1000.0,1.0", (output / "summary.csv").read_text())
        stored = json.loads((output / "run.json").read_text())
        self.assertEqual(stored["schema_version"], SCHEMA_VERSION)
        self.assertEqual(stored["state"], "passed")
        self.assertEqual(stored["benchmark"], "ping-bench")
        self.assertEqual(stored["affinity"][0]["mode"], "none")
        self.assertEqual(stored["binary"]["sha256"], self._binary(script).sha256)
        for threads in (1, 2):
            for index in range(1, 4):
                repetition = output / "none" / "threads-{:03d}".format(threads) / "repeat-{:03d}".format(index)
                self.assertTrue((repetition / "stdout.txt").is_file())
                self.assertTrue((repetition / "stderr.txt").is_file())
                self.assertTrue((repetition / "metrics.csv").is_file())

    def test_star_run_selects_star_filter_environment_and_summary(self):
        """Select the star filter, pass stars and duration, then render a star-specific summary."""
        script = self._script("""
            test "$1" = "HeavyActorBenchmark::StarSendActivateReceiveCSVManual" || exit 10
            test "$ACTORSYSTEM_STARS" = "2,4" || exit 11
            test "$ACTORSYSTEM_DURATION" = "3" || exit 12
            echo "threads,actorPairs,star_multiply,msgs_per_sec,elapsed_seconds,min_pair_sent_msgs,max_pair_sent_msgs"
            if test "$ACTORSYSTEM_THREADS" = "1"; then
              echo "1,32,2,1000,3.0,900,1100"
              echo "1,32,4,2000,3.0,1800,2200"
            else
              echo "2,32,2,3000,3.0,2800,3200"
              echo "2,32,4,4000,3.0,3800,4200"
            fi
            """)
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
        benchmark = self._script("""
            echo "threads,actorPairs,in_flight,msgs_per_sec,elapsed_seconds,min_pair_sent_msgs,max_pair_sent_msgs"
            if test "$ACTORSYSTEM_THREADS" = "1"; then
              echo "1,32,1,1000,1.0,900,1100"
            else
              echo "2,32,1,2000,1.0,1800,2200"
            fi
            """)
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
        repetition = output / "none" / "threads-001" / "repeat-001"
        self.assertTrue((repetition / "perf.data").is_file())
        self.assertIn("HotFunction", (repetition / "perf-report.txt").read_text())
        self.assertIn("0123456789abcdef", (repetition / "perf-buildids.txt").read_text())
        run = manifest["runs"][0]
        self.assertEqual(run["perf_data"], "none/threads-001/repeat-001/perf.data")
        self.assertEqual([record["name"] for record in run["perf_postprocessing"]], ["report", "buildid-list"])

    def test_empty_csv_fails_even_with_zero_exit_code(self):
        script = self._script("""
            echo "threads,actorPairs,in_flight,msgs_per_sec,elapsed_seconds,min_pair_sent_msgs,max_pair_sent_msgs"
            """)
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
        self.assertFalse((output / "none" / "threads-001" / "repeat-001").exists())

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

    def test_affinity_uses_taskset_without_preexec_fn(self):
        process = mock.Mock()
        process.communicate.return_value = ("output", "")
        process.returncode = 0
        command = ("benchmark", "--flag", "value with spaces")

        with mock.patch.object(os, "sched_setaffinity", create=True), mock.patch.object(
            runner.shutil, "which", return_value="/usr/bin/taskset"
        ), mock.patch.object(runner.subprocess, "Popen", return_value=process) as popen:
            result = run_command(command, {}, timeout_seconds=1, cpu_affinity=(4, 2, 4))

        self.assertEqual(
            popen.call_args.args[0],
            ("/usr/bin/taskset", "--cpu-list", "2,4", "benchmark", "--flag", "value with spaces"),
        )
        self.assertNotIn("preexec_fn", popen.call_args.kwargs)
        self.assertTrue(popen.call_args.kwargs["start_new_session"])
        self.assertEqual(result.command, command)

    def test_managed_process_guard_receives_expected_parent_pid(self):
        process = mock.Mock()
        command = ("benchmark", "--flag")

        with mock.patch.object(runner.os, "getpid", return_value=4321), mock.patch.object(
            runner.subprocess, "Popen", return_value=process
        ) as popen:
            managed = runner.start_managed_process(
                command,
                self.root / "stdout.txt",
                self.root / "stderr.txt",
                parent_death_wrapper=self.root / "process_guard",
            )

        try:
            self.assertEqual(
                popen.call_args.args[0],
                (str(self.root / "process_guard"), "4321", "benchmark", "--flag"),
            )
            self.assertEqual(managed.command, command)
        finally:
            managed.stdout_file.close()
            managed.stderr_file.close()

    def test_local_ydb_requires_linux(self):
        with mock.patch.object(local_ydb.sys, "platform", "darwin"), self.assertRaisesRegex(
            BenchmarkError, "require Linux"
        ):
            local_ydb.run_local_ydb({}, None, self.root, {})

    @unittest.skipUnless(
        hasattr(os, "sched_getaffinity") and shutil.which("taskset"),
        "requires Linux taskset",
    )
    def test_taskset_applies_requested_affinity(self):
        cpu = min(os.sched_getaffinity(0))
        script = self._script('taskset --pid --cpu-list "$$"')
        result = run_command(
            [script],
            {},
            timeout_seconds=5,
            cpu_affinity=(cpu,),
        )

        self.assertEqual(result.exit_code, 0, result.stderr)
        self.assertEqual(result.stdout.rsplit(":", 1)[-1].strip(), str(cpu))

    def test_cpu_list_parser(self):
        self.assertEqual(parse_cpu_list("0-3,8,10-11\n"), (0, 1, 2, 3, 8, 10, 11))

    def test_darwin_topology_uses_device_tree_clusters(self):
        entries = []
        for cpu, cluster, kind in ((0, 0, "E"), (1, 0, "E"), (2, 1, "P"), (3, 1, "P"), (4, 2, "P")):
            entries.append(
                {
                    "logical-cpu-id": cpu,
                    "cluster-id": cluster.to_bytes(4, byteorder="little"),
                    "cluster-type": kind.encode("ascii") + b"\0",
                }
            )
        entries.append(entries[0].copy())
        topology = _parse_darwin_topology(
            plistlib.dumps([{"IORegistryEntryChildren": entries}]),
            (0, 1, 2, 3, 4),
        )

        self.assertEqual(topology.chiplets, ((0, (0, 1)), (0, (2, 3)), (0, (4,))))
        self.assertEqual(
            topology.chiplet_labels,
            (
                ((0, 1), "Efficiency cluster"),
                ((2, 3), "Performance cluster 1"),
                ((4,), "Performance cluster 2"),
            ),
        )
        self.assertEqual(topology.physical_cores, ((0,), (1,), (2,), (3,), (4,)))
        self.assertEqual(topology.smt_siblings, topology.physical_cores)
        self.assertEqual(topology_record(topology)["chiplets"][0]["label"], "Efficiency cluster")

    def test_darwin_topology_rejects_conflicting_cpu_entries(self):
        entries = [
            {"logical-cpu-id": 0, "cluster-id": 0, "cluster-type": b"E\0"},
            {"logical-cpu-id": 0, "cluster-id": 1, "cluster-type": b"P\0"},
        ]

        self.assertIsNone(
            _parse_darwin_topology(
                plistlib.dumps([{"IORegistryEntryChildren": entries}]),
                (0,),
            )
        )

    def test_darwin_topology_discovery_times_out(self):
        with mock.patch.object(
            topology.subprocess,
            "run",
            side_effect=topology.subprocess.TimeoutExpired("ioreg", 10),
        ) as run:
            self.assertIsNone(topology._discover_darwin_topology((0,)))

        self.assertEqual(run.call_args.kwargs["timeout"], 10)

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
        self.assertEqual(
            record["numa_nodes"],
            [
                {"id": 0, "cpus": [1, 2, 3]},
                {"id": 1, "cpus": [4]},
            ],
        )
        self.assertEqual(
            record["chiplets"],
            [
                {"numa_node": 0, "cpus": [1]},
                {"numa_node": 0, "cpus": [2, 3]},
            ],
        )

    def test_topology_hierarchy_from_synthetic_sysfs(self):
        cases = (
            {
                "name": "single_numa_smt",
                "nodes": ((0, "0-3"),),
                "l3": ((0, "0-3"),),
                "cpu_data": ((0, 0, 0, "0-1"), (1, 0, 0, "0-1"), (2, 0, 1, "2-3"), (3, 0, 1, "2-3")),
                "allowed": (0, 1, 2, 3),
                "cores": ((0, 1), (2, 3)),
                "siblings": ((0, 1), (2, 3)),
                "reasons": (),
            },
            {
                "name": "multi_numa_multiple_chiplets_smt_off",
                "nodes": ((0, "0-1"), (1, "2-3")),
                "l3": ((0, "0-1"), (1, "2-3")),
                "cpu_data": ((0, 0, 0, "0"), (1, 0, 1, "1"), (2, 1, 0, "2"), (3, 1, 1, "3")),
                "allowed": (0, 1, 2, 3),
                "cores": ((0,), (1,), (2,), (3,)),
                "siblings": ((0,), (1,), (2,), (3,)),
                "reasons": (),
            },
            {
                "name": "asymmetric_cpuset",
                "nodes": ((0, "0-3"),),
                "l3": ((1, "0-1"), (2, "2-3")),
                "cpu_data": ((0, 0, 0, "0-1"), (1, 0, 0, "0-1"), (2, 0, 1, "2-3"), (3, 0, 1, "2-3")),
                "allowed": (1, 2),
                "cores": ((1,), (2,)),
                "siblings": ((1,), (2,)),
                "reasons": (),
            },
            {
                "name": "missing_numa_l3_and_incomplete_topology",
                "nodes": (),
                "l3": (),
                "cpu_data": ((0, None, None, None), (1, 0, 1, None)),
                "allowed": (0, 1),
                "cores": ((0,), (1,)),
                "siblings": ((0,), (1,)),
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

    def test_partial_l3_topology_disables_only_chiplet_affinity_modes(self):
        sys_root = self.root / "partial-l3" / "sys" / "devices" / "system"
        for node_id, cpus in ((0, "0-1"), (1, "2-3")):
            node = sys_root / "node" / "node{}".format(node_id)
            node.mkdir(parents=True)
            node.joinpath("cpulist").write_text(cpus, encoding="utf-8")
        for cpu in (0, 1):
            cache = sys_root / "cpu" / "cpu{}".format(cpu) / "cache" / "index3"
            cache.mkdir(parents=True)
            cache.joinpath("level").write_text("3", encoding="utf-8")
            cache.joinpath("shared_cpu_list").write_text("0-1", encoding="utf-8")

        topology = discover_topology(sys_root, allowed_cpus=(0, 1, 2, 3))

        self.assertEqual(topology.chiplets, ((0, (0, 1)),))
        self.assertIn("do not cover all allowed CPUs", topology.chiplet_topology_reason)
        self.assertIn("missing: 2, 3", topology.chiplet_topology_reason)
        self.assertIn(("chiplet", topology.chiplet_topology_reason), topology.hierarchy_reasons)
        with mock.patch.object(os, "sched_setaffinity", create=True):
            self.assertTrue(plan_affinity("pack-numa", topology, 1).supported)
            placement = plan_affinity("pack-numa-pack-chiplet", topology, 1)
        self.assertFalse(placement.supported)
        self.assertIn("chiplet-based affinity is unavailable", placement.reason)

    def test_cross_numa_l3_group_disables_chiplet_affinity_modes(self):
        sys_root = self.root / "cross-numa-l3" / "sys" / "devices" / "system"
        for node_id, cpus in ((0, "0-1"), (1, "2-3")):
            node = sys_root / "node" / "node{}".format(node_id)
            node.mkdir(parents=True)
            node.joinpath("cpulist").write_text(cpus, encoding="utf-8")
        for cpu in range(4):
            cache = sys_root / "cpu" / "cpu{}".format(cpu) / "cache" / "index3"
            cache.mkdir(parents=True)
            cache.joinpath("level").write_text("3", encoding="utf-8")
            cache.joinpath("shared_cpu_list").write_text("0-3", encoding="utf-8")

        topology = discover_topology(sys_root, allowed_cpus=(0, 1, 2, 3))

        self.assertEqual(topology.chiplets, ())
        self.assertIn("does not belong to exactly one NUMA node", topology.chiplet_topology_reason)
        self.assertIn(("chiplet", topology.chiplet_topology_reason), topology.hierarchy_reasons)
        with mock.patch.object(os, "sched_setaffinity", create=True):
            self.assertTrue(plan_affinity("pack-numa", topology, 1).supported)
            placement = plan_affinity("pack-numa-pack-chiplet", topology, 1)
        self.assertFalse(placement.supported)
        self.assertIn("chiplet-based affinity is unavailable", placement.reason)

    def test_affinity_modes_select_compositional_deterministic_masks(self):
        topology = CpuTopology(
            allowed_cpus=tuple(range(16)),
            numa_nodes=((0, tuple(range(8))), (1, tuple(range(8, 16)))),
            chiplets=((0, (0, 1, 2, 3)), (0, (4, 5, 6, 7)), (1, (8, 9, 10, 11)), (1, (12, 13, 14, 15))),
            physical_cores=((0, 1), (2, 3), (4, 5), (6, 7), (8, 9), (10, 11), (12, 13), (14, 15)),
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
            self.assertEqual(plan_affinity("pack-numa-spread-chiplet-spread-core", topology, 3).cpus, (1, 2, 4))
            unsupported = plan_affinity("spread-numa-pack-chiplet", topology, 2)
        self.assertFalse(unsupported.supported)
        self.assertIn("spread-numa", unsupported.reason)

    def test_all_affinity_modes_with_smt_disabled(self):
        topology = CpuTopology(
            allowed_cpus=tuple(range(16)),
            numa_nodes=((0, tuple(range(8))), (1, tuple(range(8, 16)))),
            chiplets=((0, (0, 1, 2, 3)), (0, (4, 5, 6, 7)), (1, (8, 9, 10, 11)), (1, (12, 13, 14, 15))),
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

    def test_unpinned_all_numa_background_requires_multiple_numa_nodes(self):
        single_node = CpuTopology(
            allowed_cpus=(0, 1, 2, 3),
            numa_nodes=((0, (0, 1, 2, 3)),),
            chiplets=((0, (0, 1, 2, 3)),),
            physical_cores=((0,), (1,), (2,), (3,)),
        )
        unsupported = plan_background_load("coherence-all-numa", single_node, None, 1)
        self.assertFalse(unsupported.supported)
        self.assertIn("at least two NUMA nodes", unsupported.reason)

        two_nodes = replace(
            single_node,
            numa_nodes=((0, (0, 1)), (1, (2, 3))),
            chiplets=((0, (0, 1)), (1, (2, 3))),
        )
        supported = plan_background_load("coherence-all-numa", two_nodes, None, 1)
        self.assertTrue(supported.supported)
        self.assertEqual(supported.workers, 2)

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

    def test_run_skips_when_all_affinity_modes_are_unsupported(self):
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

        with mock.patch.object(actors_core, "discover_topology", return_value=topology), mock.patch.object(
            os, "sched_setaffinity", create=True
        ):
            result = run_actors_core(
                self._binary(script),
                configuration,
                output,
                tool_revision={"commit_id": "test"},
            )

        manifest = json.loads((output / "run.json").read_text())
        self.assertEqual(result["status"], "unsupported")
        self.assertEqual(manifest["status"], "unsupported")
        self.assertEqual(manifest["state"], "unsupported")
        self.assertIn("finished_at", manifest)
        self.assertIn("unsupported", manifest["error"])
        self.assertEqual(manifest["runs"], [])
        self.assertEqual(manifest["affinity"][0]["status"], "unsupported")
        self.assertTrue((output / "summary.csv").exists())
        self.assertEqual(manifest["summary"], "summary.csv")
        self.assertEqual(manifest["repetitions"], "repetitions.csv")
        self.assertEqual(manifest["summary_rows"], 0)


class WebTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory(prefix="ydb-bench-web-test-")
        self.root = Path(self.temporary_directory.name)

    def tearDown(self):
        self.temporary_directory.cleanup()

    def _manifest(self, directory, status="completed", imported=False):
        directory.mkdir(parents=True, exist_ok=True)
        value = {
            "schema_version": SCHEMA_VERSION,
            "status": status,
            "state": "running" if status == "running" else "passed",
            "started_at": "2025-01-01T00:00:00+00:00",
            "runs": [{"benchmark": "ping-bench", "profile": "baseline", "status": status}],
            "steps": [
                {
                    "id": "step-1",
                    "benchmark": "ping-bench",
                    "profile": "baseline",
                    "affinity": "none",
                    "threads": 1,
                    "case": 1,
                    "parameters": {},
                    "repeat": 1,
                    "state": "running" if status == "running" else "passed",
                    "artifacts": ["artifact.txt"],
                }
            ],
            "config": {
                "snapshot": "ping-bench:\n  baseline: {threads: [1], duration: 1, repetitions: 1, affinity: [none]}\n",
            },
            "topology": {
                "version": 2,
                "allowed_cpus": [0],
                "numa_nodes": [{"id": 0, "cpus": [0]}],
                "chiplets": [{"numa_node": 0, "cpus": [0]}],
                "physical_cores": [[0]],
                "smt_siblings": [[0]],
                "hierarchy_reasons": [],
            },
        }
        if status != "running":
            value["finished_at"] = "2025-01-01T00:00:01+00:00"
        if imported:
            value["imported"] = True
        (directory / "run.json").write_text(json.dumps(value), encoding="utf-8")
        (directory / "artifact.txt").write_text("artifact", encoding="utf-8")

    def _local_ydb_result(self, directory, throughput, operation="put"):
        self._manifest(directory)
        main_path = directory / "run.json"
        main = json.loads(main_path.read_text(encoding="utf-8"))
        relative = Path("local-ydb") / "capacity"
        main["runs"] = [
            {
                "benchmark": "local-ydb",
                "profile": "capacity",
                "status": "completed",
                "directory": str(relative),
                "manifest": str(relative / "run.json"),
            }
        ]
        main["steps"] = [
            {
                "id": "step-1",
                "benchmark": "local-ydb",
                "profile": "capacity",
                "affinity": "roles",
                "threads": 64,
                "case": 1,
                "parameters": {},
                "repeat": 1,
                "state": "passed",
                "artifacts": [str(relative / "run.json")],
            }
        ]
        main_path.write_text(json.dumps(main), encoding="utf-8")
        profile_root = directory / relative
        profile_root.mkdir(parents=True)
        selected_metrics = {
            "throughput": throughput,
            "p99_ms": 10,
            "errors": 0,
            "static_cpu_mean": 50,
            "dynamic_cpu_mean": 80,
            "cli_cpu_mean": 30,
            "commands": [{"argv": ["must", "not", "leak"]}],
        }
        profile = {
            "schema_version": SCHEMA_VERSION,
            "benchmark": "local-ydb",
            "profile": "capacity",
            "status": "completed",
            "state": "passed",
            "started_at": "2025-01-01T00:00:00+00:00",
            "finished_at": "2025-01-01T00:01:00+00:00",
            "tool_revision": "test-revision",
            "binaries": {
                "ydbd": {"sha256": "server-sha"},
                "ydb_cli": {"sha256": "cli-sha"},
                "process_guard": {"sha256": "guard-sha", "private": "not projected"},
            },
            "platform": {
                "cpu_model": "test CPU",
                "uname": {"node": "test-host", "release": "test-kernel"},
                "private": "not projected",
            },
            "cpu_topology": {"version": 2, "allowed_cpus": [0, 1, 2], "private": "not projected"},
            "parameters": {
                "workload": {"type": "kv", "operation": operation, "options": {"partitions": 16}},
                "geometry": {
                    "preset": "single",
                    "static_nodes": 1,
                    "dynamic_nodes": 1,
                    "max_dynamic_nodes": 1,
                    "storage_groups": 1,
                    "disk_size_gb": 64,
                },
                "client": {"threads": 64},
                "load": {"parameter": "rate", "values": [100]},
                "measurement": {"warmup": 1, "duration": 10, "repetitions": 3},
                "private": "not projected",
            },
            "role_affinity": {"ydb_cli": [0], "static_nodes": [1], "dynamic_nodes": [2]},
            "attempts": [],
            "searches": [],
            "result": {
                "outcome": "best-observed",
                "parameter": "rate",
                "dynamic_nodes": 1,
                "selected_load": 100,
                "selected_metrics": selected_metrics,
            },
        }
        (profile_root / "run.json").write_text(json.dumps(profile), encoding="utf-8")

    def _portable_archive(self, extra=None, version=SCHEMA_VERSION, corrupt=False, run_updates=None):
        run = {
            "schema_version": version,
            "status": "completed",
            "state": "passed",
            "started_at": "2025-01-01T00:00:00+00:00",
            "finished_at": "2025-01-01T00:00:01+00:00",
            "runs": [],
            "steps": [
                {
                    "id": "step-1",
                    "benchmark": "ping-bench",
                    "profile": "baseline",
                    "affinity": "none",
                    "threads": 1,
                    "case": 1,
                    "parameters": {},
                    "repeat": 1,
                    "state": "passed",
                    "artifacts": ["artifact.txt"],
                }
            ],
            "topology": {
                "version": 2,
                "allowed_cpus": [0],
                "numa_nodes": [{"id": 0, "cpus": [0]}],
                "chiplets": [{"numa_node": 0, "cpus": [0]}],
                "physical_cores": [[0]],
                "smt_siblings": [[0]],
                "hierarchy_reasons": [],
            },
        }
        run.update(run_updates or {})
        files = {"run.json": json.dumps(run).encode(), "artifact.txt": b"artifact"}
        entries = [
            {"path": name, "sha256": hashlib.sha256(data).hexdigest(), "size": len(data)}
            for name, data in files.items()
        ]
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

    def test_import_rejects_nonterminal_and_malformed_run_manifests(self):
        with self.assertRaisesRegex(BenchmarkError, "state must be terminal"):
            import_archive(self.root, self._portable_archive(run_updates={"status": "running", "state": "running"}))
        with self.assertRaisesRegex(BenchmarkError, "steps must be a list"):
            import_archive(self.root, self._portable_archive(run_updates={"steps": {}}))
        with self.assertRaisesRegex(BenchmarkError, "topology is missing"):
            import_archive(self.root, self._portable_archive(run_updates={"topology": {}}))
        with self.assertRaisesRegex(BenchmarkError, "status must be terminal"):
            import_archive(
                self.root,
                self._portable_archive(
                    run_updates={
                        "runs": [
                            {
                                "benchmark": "ping-bench",
                                "profile": "baseline",
                                "status": "running",
                            }
                        ]
                    }
                ),
            )
        malformed_step = {
            "id": "step-1",
            "benchmark": "ping-bench",
            "profile": "baseline",
            "affinity": "none",
            "threads": 1,
            "case": 1,
            "parameters": {},
            "repeat": 1,
            "state": "passed",
            "artifacts": ["not-in-archive.txt"],
        }
        with self.assertRaisesRegex(BenchmarkError, "does not name a file in the archive"):
            import_archive(self.root, self._portable_archive(run_updates={"steps": [malformed_step]}))

    def test_import_rejects_invalid_case_indices(self):
        for case in (True, 0, 1.5):
            with self.subTest(case=case), self.assertRaisesRegex(
                BenchmarkError, r"steps\[0\]\.case must be an integer greater than or equal to 1"
            ):
                step = {
                    "id": "step-1",
                    "benchmark": "ping-bench",
                    "profile": "baseline",
                    "affinity": "none",
                    "threads": 1,
                    "case": case,
                    "parameters": {},
                    "repeat": 1,
                    "state": "passed",
                    "artifacts": ["artifact.txt"],
                }
                import_archive(self.root, self._portable_archive(run_updates={"steps": [step]}))

    def test_import_rejects_duplicate_step_ids(self):
        step = {
            "id": "same",
            "benchmark": "ping-bench",
            "profile": "baseline",
            "affinity": "none",
            "threads": 1,
            "case": 1,
            "parameters": {},
            "repeat": 1,
            "state": "passed",
            "artifacts": [],
        }
        with self.assertRaisesRegex(BenchmarkError, "duplicate step id"):
            import_archive(self.root, self._portable_archive(run_updates={"steps": [step, dict(step)]}))

    def test_import_installs_immutable_normalized_result(self):
        imported = import_archive(self.root, self._portable_archive())
        self.assertEqual(imported["source"], "imported")
        run = self.root / imported["id"]
        self.assertEqual(read_model(self.root)[imported["id"]]["source"], "imported")
        self.assertFalse((run / "run.json").stat().st_mode & stat.S_IWUSR)
        with self.assertRaises(FileExistsError):
            (run / "artifact.txt").open("x")

    def test_import_removes_read_only_staging_after_atomic_install_failure(self):
        with mock.patch.object(import_results.os, "replace", side_effect=OSError("injected replace failure")):
            with self.assertRaisesRegex(OSError, "injected replace failure"):
                import_archive(self.root, self._portable_archive())
        self.assertEqual(list((self.root / "imports").glob(".import-*")), [])

    def test_export_uses_the_same_portable_archive_contract(self):
        self._manifest(self.root / "complete")
        destination = self.root / "other-host"
        with export_archive(self.root / "complete") as archive:
            imported = import_archive(destination, archive.read_bytes())
        self.assertEqual(imported["source"], "imported")
        self.assertEqual((destination / imported["id"] / "artifact.txt").read_text(), "artifact")

    def test_web_archive_streams_from_temporary_file_and_cleans_it(self):
        self._manifest(self.root / "complete")
        (self.root / "complete" / "large.bin").write_bytes(b"x" * (web._STREAM_CHUNK_SIZE * 2 + 17))
        server = make_server("127.0.0.1", 0, self.root)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        temporary_paths = []
        read_sizes = []
        copy_stream = web._copy_stream

        def track_copy(source, destination):
            temporary_paths.append(Path(source.name))

            class TrackedSource:
                def read(self, size):
                    read_sizes.append(size)
                    return source.read(size)

            copy_stream(TrackedSource(), destination)

        try:
            base = "http://127.0.0.1:{}".format(server.server_port)
            with mock.patch.object(web, "_copy_stream", side_effect=track_copy), mock.patch.object(
                Path, "read_bytes", side_effect=AssertionError("archive must not materialize files")
            ):
                with urllib.request.urlopen(base + "/api/runs/complete/archive") as response:
                    self.assertTrue(response.read().startswith(b"PK"))
            self.assertEqual(len(temporary_paths), 1)
            self.assertTrue(temporary_paths[0].name.startswith("ydb-bench-export-"))
            deadline = time.monotonic() + 2
            while temporary_paths[0].exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertFalse(temporary_paths[0].exists())
            self.assertTrue(read_sizes)
            self.assertEqual(set(read_sizes), {web._STREAM_CHUNK_SIZE})
        finally:
            server.shutdown()
            worker.join()
            server.server_close()

    def test_run_service_events_rejects_path_traversal_for_persisted_runs(self):
        with tempfile.TemporaryDirectory(prefix="ydb-bench-events-outside-", dir=self.root.parent) as outside:
            outside_path = Path(outside)
            (outside_path / "events.jsonl").write_text('{"sequence":1,"type":"outside"}\n', encoding="utf-8")
            service = RunService(self.root)
            with self.assertRaisesRegex(BenchmarkError, "run not found"):
                service.events("../" + outside_path.name)

    def test_run_service_events_decodes_each_persisted_line_once(self):
        self._manifest(self.root / "complete")
        (self.root / "complete" / "events.jsonl").write_text(
            '{"sequence":1,"type":"first"}\n' '{"sequence":2,"type":"second","throughput_gain_percent":Infinity}\n',
            encoding="utf-8",
        )
        service = RunService(self.root)
        loads = json.loads
        with mock.patch.object(web.json, "loads", wraps=loads) as decode:
            events = service.events("complete", after=1)
        self.assertEqual([event["type"] for event in events], ["second"])
        self.assertIsNone(events[0]["throughput_gain_percent"])
        self.assertEqual(decode.call_count, 2)

    def test_run_service_migrates_non_finite_manifest_values_on_recovery(self):
        run_root = self.root / "non-finite-recovery"
        self._manifest(run_root, "running")
        path = run_root / "run.json"
        manifest = json.loads(path.read_text(encoding="utf-8"))
        manifest["attempts"] = [{"throughput_gain_percent": float("inf")}]
        path.write_text(json.dumps(manifest), encoding="utf-8")

        service = RunService(self.root)
        try:
            stored_text = path.read_text(encoding="utf-8")
            stored = json.loads(stored_text)
            self.assertNotIn("Infinity", stored_text)
            self.assertIsNone(stored["attempts"][0]["throughput_gain_percent"])
            self.assertEqual(stored["state"], "recovery_required")
        finally:
            service.shutdown()

    def test_run_service_replays_events_evicted_from_live_deque(self):
        def fake_executor(run, emit, _cancelled):
            step_id = run["store"].manifest["steps"][0]["id"]
            emit({"type": "step-started", "step_id": step_id})
            for index in range(5):
                emit({"type": "step-progress", "step_id": step_id, "fields": {"progress": {"index": index}}})
            emit({"type": "step-finished", "step_id": step_id, "state": "passed"})

        service = RunService(self.root, executor=fake_executor, event_limit=2)
        try:
            run_id = service.start(
                "ping-bench:\n  replay: {threads: [1], duration: 1, repetitions: 1, affinity: [none]}\n"
            )["id"]
            run = service._runs[run_id]
            self.assertTrue(run["finished"].wait(2))
            replayed = service.events(run_id, after=0)
            persisted = [json.loads(line) for line in (run["root"] / "events.jsonl").read_text().splitlines()]
            self.assertGreater(len(replayed), service.event_limit)
            self.assertEqual(replayed, persisted)
            self.assertEqual(service.events(run_id, after=replayed[-3]["sequence"]), replayed[-2:])
        finally:
            service.shutdown()

    def test_local_ydb_profile_projection_supports_preparing_and_live_results(self):
        run_root = self.root / "local-ydb-run"
        self._manifest(run_root, "running")
        main_path = run_root / "run.json"
        main = json.loads(main_path.read_text(encoding="utf-8"))
        main.update({"status": "queued", "state": "queued"})
        main["runs"] = []
        main["steps"] = [
            {
                "id": "step-1",
                "benchmark": "local-ydb",
                "profile": "capacity",
                "affinity": "roles",
                "threads": 64,
                "case": 1,
                "parameters": {},
                "repeat": 1,
                "state": "pending",
                "artifacts": [],
            }
        ]
        main_path.write_text(json.dumps(main), encoding="utf-8")
        service = RunService(self.root)
        try:
            self.assertEqual(service.local_ydb_profile("local-ydb-run", "capacity")["state"], "preparing")
            relative = Path("local-ydb") / "capacity"
            profile_root = run_root / relative
            profile_root.mkdir(parents=True)
            main["runs"] = [
                {
                    "benchmark": "local-ydb",
                    "profile": "capacity",
                    "status": "running",
                    "directory": str(relative),
                }
            ]
            main_path.write_text(json.dumps(main), encoding="utf-8")
            profile_manifest = {
                "schema_version": SCHEMA_VERSION,
                "benchmark": "local-ydb",
                "profile": "capacity",
                "status": "running",
                "state": "running",
                "started_at": "2025-01-01T00:00:00+00:00",
                "parameters": {"load": {"parameter": "rate", "values": [10]}},
                "timeout_seconds": 300,
                "role_affinity": {"ydb_cli": [0, 128], "static_nodes": None, "dynamic_nodes": [1, 2]},
                "progress": {
                    "phase": "measuring",
                    "attempt": 1,
                    "load": 10,
                    "current_command": {
                        "argv": ["/tmp/ydb cli", "workload", "stock", "run", "add-rand-order"],
                        "cpu_affinity": [0, 128],
                    },
                },
                "attempts": [
                    {
                        "attempt": 1,
                        "load": 10,
                        "passed": True,
                        "commands": [
                            {
                                "phase": "measuring",
                                "repetition": 1,
                                "argv": ["/tmp/ydb cli", "workload", "stock", "run", "add-rand-order"],
                                "cpu_affinity": [0, 128],
                            }
                        ],
                    }
                ],
                "searches": [],
            }
            (profile_root / "run.json").write_text(json.dumps(profile_manifest), encoding="utf-8")
            projected = service.local_ydb_profile("local-ydb-run", "capacity")
            self.assertEqual(projected["progress"]["phase"], "measuring")
            self.assertEqual(projected["progress"]["current_command"]["argv"][3], "run")
            self.assertEqual(projected["attempts"][0]["load"], 10)
            self.assertEqual(projected["attempts"][0]["commands"][0]["argv"][-1], "add-rand-order")
            self.assertEqual(projected["timeout_seconds"], 300)
            self.assertEqual(projected["role_affinity"]["dynamic_nodes"], [1, 2])

            main.update({"status": "recovery_required", "state": "recovery_required"})
            main_path.write_text(json.dumps(main), encoding="utf-8")
            recovered = service.local_ydb_profile("local-ydb-run", "capacity")
            self.assertEqual(recovered["status"], "recovery_required")
            self.assertEqual(recovered["state"], "recovery_required")
        finally:
            service.shutdown()

    def test_local_ydb_comparison_returns_bounded_profile_results(self):
        self._local_ydb_result(self.root / "baseline", 1000)
        self._local_ydb_result(self.root / "candidate", 1100, operation="mixed")
        service = RunService(self.root)
        try:
            comparison = service.local_ydb_comparison(["baseline", "candidate"])
            self.assertEqual(
                [(item["run"], item["profile"]) for item in comparison["entries"]],
                [("baseline", "capacity"), ("candidate", "capacity")],
            )
            self.assertEqual(comparison["entries"][0]["result"]["selected_metrics"]["throughput"], 1000)
            self.assertEqual(comparison["entries"][0]["binaries"]["ydbd"]["sha256"], "server-sha")
            self.assertEqual(comparison["entries"][0]["cpu_topology"]["allowed_cpus"], [0, 1, 2])
            self.assertNotIn("process_guard", comparison["entries"][0]["binaries"])
            self.assertNotIn("private", comparison["entries"][0]["platform"])
            self.assertNotIn("private", comparison["entries"][0]["cpu_topology"])
            self.assertNotIn("private", comparison["entries"][0]["parameters"])
            self.assertNotIn("attempts", comparison["entries"][0])
            self.assertNotIn("commands", comparison["entries"][0]["result"]["selected_metrics"])
            self.assertEqual(comparison["entries"][1]["parameters"]["workload"]["operation"], "mixed")
            with self.assertRaisesRegex(BenchmarkError, "between 1 and 20"):
                service.local_ydb_comparison([])
            with self.assertRaisesRegex(BenchmarkError, "between 1 and 20"):
                service.local_ydb_comparison(["baseline"] * 21)
            with self.assertRaisesRegex(BenchmarkError, "must be unique"):
                service.local_ydb_comparison(["baseline", "baseline"])
            with self.assertRaisesRegex(BenchmarkError, "run not found"):
                service.local_ydb_comparison(["missing"])
            with mock.patch.object(
                service,
                "local_ydb_profile",
                return_value={"tool_revision": "x" * (4 * 1024 * 1024)},
            ):
                with self.assertRaisesRegex(BenchmarkError, "response is too large"):
                    service.local_ydb_comparison(["baseline"])
        finally:
            service.shutdown()

    def test_local_ydb_profile_projection_preserves_terminal_state_without_nested_manifest(self):
        run_root = self.root / "terminal-local-ydb-run"
        self._manifest(run_root)
        main_path = run_root / "run.json"
        main = json.loads(main_path.read_text(encoding="utf-8"))
        main.update({"status": "cancelled", "state": "cancelled"})
        main["runs"] = []
        main["steps"] = [
            {
                "id": "step-1",
                "benchmark": "local-ydb",
                "profile": "capacity",
                "affinity": "roles",
                "threads": 64,
                "case": 1,
                "parameters": {},
                "repeat": 1,
                "state": "cancelled",
                "reason": "cancelled before startup",
                "artifacts": [],
            }
        ]
        main_path.write_text(json.dumps(main), encoding="utf-8")
        service = RunService(self.root)
        try:
            cancelled = service.local_ydb_profile("terminal-local-ydb-run", "capacity")
            self.assertEqual(cancelled["status"], "cancelled")
            self.assertEqual(cancelled["state"], "cancelled")

            main.update({"status": "failed", "state": "failed", "error": "startup failed"})
            main["runs"] = [
                {
                    "benchmark": "local-ydb",
                    "profile": "capacity",
                    "status": "failed",
                    "directory": "local-ydb/capacity",
                    "error": "startup failed",
                }
            ]
            main["steps"][0].update({"state": "failed", "error": "startup failed"})
            main_path.write_text(json.dumps(main), encoding="utf-8")
            failed = service.local_ydb_profile("terminal-local-ydb-run", "capacity")
            self.assertEqual(failed["status"], "failed")
            self.assertEqual(failed["state"], "failed")
            self.assertEqual(failed["error"], "startup failed")
        finally:
            service.shutdown()

    def test_download_content_disposition_encodes_hostile_run_and_artifact_names(self):
        run_id = 'run"\r\n\\é'
        artifact_name = 'report"\r\n\\é.txt'
        self._manifest(self.root / run_id)
        (self.root / run_id / artifact_name).write_text("artifact", encoding="utf-8")
        server = make_server("127.0.0.1", 0, self.root)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            base = "http://127.0.0.1:{}".format(server.server_port)
            paths = (
                "/api/runs/{}/config".format(quote(run_id, safe="")),
                "/api/runs/{}/artifact/{}".format(quote(run_id, safe=""), quote(artifact_name, safe="")),
            )
            for path in paths:
                with self.subTest(path=path), urllib.request.urlopen(base + path) as response:
                    disposition = response.headers["Content-Disposition"]
                    disposition.encode("ascii")
                    self.assertEqual(disposition.count('"'), 2)
                    self.assertNotIn("\r", disposition)
                    self.assertNotIn("\n", disposition)
                    self.assertNotIn("\\", disposition)
                    self.assertNotIn("é", disposition)
                    self.assertIn("filename*=UTF-8''", disposition)
                    self.assertIn("%22%0D%0A%5C%C3%A9", disposition)
        finally:
            server.shutdown()
            worker.join()
            server.server_close()

    def test_events_endpoint_rejects_non_integer_after_query(self):
        self._manifest(self.root / "complete")
        server = make_server("127.0.0.1", 0, self.root)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            base = "http://127.0.0.1:{}".format(server.server_port)
            with self.assertRaises(HTTPError) as caught:
                urllib.request.urlopen(base + "/api/runs/complete/events?after=abc")
            self.assertEqual(caught.exception.code, 400)
            self.assertIn("must be an integer", caught.exception.read().decode())
        finally:
            server.shutdown()
            worker.join()
            server.server_close()

    def test_comparison_key_rules(self):
        model = {
            "one": {
                "steps": [
                    {"benchmark": "ping", "profile": "p", "affinity": "none"},
                    {"benchmark": "ping", "profile": "q", "affinity": "pack"},
                ],
                "runs": [],
            },
            "two": {
                "steps": [
                    {"benchmark": "ping", "profile": "p", "affinity": "none"},
                    {"benchmark": "ping", "profile": "q", "affinity": "none"},
                ],
                "runs": [],
            },
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

    def test_web_runs_are_sorted_newest_first(self):
        self._manifest(self.root / "older")
        self._manifest(self.root / "newer")
        for run_id, started_at in (
            ("older", "2025-01-01T00:00:00+00:00"),
            ("newer", "2025-02-01T00:00:00+00:00"),
        ):
            path = self.root / run_id / "run.json"
            manifest = json.loads(path.read_text(encoding="utf-8"))
            manifest["started_at"] = started_at
            path.write_text(json.dumps(manifest), encoding="utf-8")
        service = RunService(self.root)
        try:
            self.assertEqual([record["id"] for record in service.filtered_model({})], ["newer", "older"])
        finally:
            service.shutdown()

    def test_chart_data_groups_summary_rows_by_affinity(self):
        self._manifest(self.root / "complete")
        summary = self.root / "complete" / "ping-bench" / "baseline" / "summary.csv"
        summary.parent.mkdir(parents=True)
        summary.write_text(
            "affinity_mode,threads,actorPairs,in_flight,repetitions,median_msgs_per_sec,min_msgs_per_sec,max_msgs_per_sec,median_elapsed_seconds\n"
            "none,1,512,1,1,10,9,11,1.0\n"
            "none,2,512,1,1,20,19,21,1.0\n"
            "pack-numa,1,512,1,1,12,11,13,1.0\n",
            encoding="utf-8",
        )
        summary.with_name("run.json").write_text(
            json.dumps(
                {
                    "affinity": [
                        {"mode": "none", "cpus": None},
                        {"mode": "pack-numa", "cpus": [0, 1, 2, 4]},
                    ],
                }
            ),
            encoding="utf-8",
        )
        value = chart_data(self.root, ["complete"])
        self.assertEqual(value["dimensions"], ["actorPairs", "in_flight", "threads"])
        self.assertIn("median_msgs_per_sec", value["metrics"])
        self.assertEqual([item["affinity"] for item in value["series"]], ["none", "pack-numa"])
        self.assertEqual(value["series"][0]["rows"][1]["threads"], 2)
        self.assertIsNone(value["series"][0]["cpus"])
        self.assertEqual(value["series"][1]["cpus"], [0, 1, 2, 4])
        self.assertIn("dimension_metadata", value)

    def test_local_ydb_summary_is_available_to_comparison_charts(self):
        self._manifest(self.root / "complete")
        summary = self.root / "complete" / "local-ydb" / "capacity" / "summary.csv"
        summary.parent.mkdir(parents=True)
        rows = []
        for load, dynamic_nodes, scale in ((10, 1, 1), (20, 1, 2), (10, 2, 3)):
            row = {"load": load, "dynamic_nodes": dynamic_nodes}
            row.update({metric.name: (index + 1) * scale for index, metric in enumerate(LOCAL_YDB_BENCHMARK.metrics)})
            rows.append(row)
        summarized = LOCAL_YDB_BENCHMARK.summarize_metrics(rows, LOCAL_YDB_BENCHMARK)
        summary.write_text(
            LOCAL_YDB_BENCHMARK.render_summary(summarized, LOCAL_YDB_BENCHMARK),
            encoding="utf-8",
        )

        value = chart_data(self.root, ["complete"])
        self.assertEqual(len(value["series"]), 1)
        self.assertEqual(value["series"][0]["benchmark"], "local-ydb")
        self.assertEqual(value["series"][0]["affinity"], "roles")
        self.assertEqual(value["series"][0]["rows"][0]["load"], 10)
        self.assertEqual(
            {(row["load"], row["dynamic_nodes"]) for row in value["series"][0]["rows"]},
            {(10, 1), (20, 1), (10, 2)},
        )
        self.assertIn("median_throughput", value["metrics"])
        self.assertIn("median_p99_ms", value["metrics"])
        self.assertIn("median_dynamic_cpu_mean", value["metrics"])
        self.assertIn("max_errors", value["metrics"])
        unrelated = self.root / "complete" / "ping-bench" / "broken" / "summary.csv"
        unrelated.parent.mkdir(parents=True)
        with unrelated.open("wb") as stream:
            stream.truncate(16 * 1024 * 1024 + 1)
        with self.assertRaisesRegex(BenchmarkError, "summary CSV is too large"):
            chart_data(self.root, ["complete"])
        filtered = chart_data(self.root, ["complete"], "local-ydb")
        self.assertEqual(len(filtered["series"]), 1)
        with self.assertRaisesRegex(BenchmarkError, "unknown chart benchmark"):
            chart_data(self.root, ["complete"], "missing")

    def test_memory_fairness_is_derived_per_repeat_before_aggregation(self):
        dimensions = ["threads", "random_percent", "scope", "worker_aggregation"]
        rows = []
        for repeat, minimum, maximum, mean in ((1, 80, 120, 100), (2, 90, 110, 100)):
            for aggregation, value in (("min", minimum), ("max", maximum), ("mean", mean)):
                rows.append(
                    {
                        "threads": 4,
                        "random_percent": 50,
                        "scope": "random",
                        "worker_aggregation": aggregation,
                        "repeat_aggregation": "raw",
                        "repeat": repeat,
                        "ops_per_sec": value,
                    }
                )
        grouped = {"none": rows}
        _add_memory_fairness_rows(grouped, dimensions)
        raw = [row for row in rows if row.get("worker_aggregation") == "fairness" and row["repeat"] != "*"]
        self.assertEqual([row["worker_max_min_spread_pct"] for row in raw], [40, 20])
        self.assertEqual([row["worker_mean_min_gap_pct"] for row in raw], [20, 10])
        median = next(
            row
            for row in rows
            if row.get("worker_aggregation") == "fairness" and row.get("repeat_aggregation") == "median"
        )
        self.assertEqual(median["worker_max_min_spread_pct"], 30)
        self.assertEqual(median["worker_mean_min_gap_pct"], 15)

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
            with urllib.request.urlopen(base + "/app.js") as response:
                script = response.read()
                self.assertIn(b"System topology", script)
                self.assertIn(b"NUMA, cache and cores", script)
                self.assertIn(b"function affinityTree", script)
                self.assertIn(b"class=affinity-tree", script)
                self.assertIn(b"SMT threads", script)
                self.assertIn(b"<span class=cpu-ranges>vCPU ", script)
                self.assertNotIn(b"(core.index+1)", script)
                self.assertIn(b"Unavailable", script)
                self.assertNotIn(b"Use in new run", script)
                self.assertNotIn(b"data-mode", script)
                self.assertNotIn(b"<th>First mask</th>", script)
                self.assertNotIn(b"Physical cores (", script)
                self.assertNotIn(b"SMT sibling sets (", script)
                self.assertIn(b"New run", script)
                self.assertIn(b"<th>Duration</th>", script)
                self.assertIn(b"<th>Runs</th>", script)
                self.assertIn(b"class=affinity-details><td colspan=3>", script)
                self.assertIn(b"id=refresh-run", script)
                self.assertIn(b"Queue position:", script)
                self.assertIn(b"Currently running:", script)
                self.assertIn(b"class=run-tabs", script)
                self.assertIn(b"profileKeys.length===1?profileKeys[0]", script)
                self.assertIn(b"class=\"card profile-overview\"", script)
                self.assertIn(b"<strong>Execution details</strong>", script)
                self.assertIn(b"<strong>Interrupted.</strong>", script)
                self.assertIn(b"<summary>Downloads</summary>", script)
                self.assertNotIn(b"['queued','running','recovery_required'].includes(run.state)", script)
                self.assertIn(b"<option>queued</option>", script)
                self.assertNotIn(b"setInterval(()=>renderRun", script)
                self.assertIn(b"function cpuRanges", script)
                self.assertIn(b"function humanTime", script)
                self.assertIn(b"dateStyle:'medium',timeStyle:'short'", script)
                self.assertIn(b"function elapsedLabel", script)
                self.assertIn(b"record.status==='running'?Date.now()", script)
                self.assertIn(b"ranges such as 1-16", script)
                self.assertIn(b"function compactIntegerRanges", script)
                self.assertIn(b"benchmarkChanged", script)
                self.assertIn(b"classList.contains('affinity')", script)
                self.assertIn(b"parameter.minimum??1", script)
                self.assertIn(b"function parameterCases", script)
                self.assertIn(b"class=parameter-choice", script)
                self.assertIn(b"Incomplete data:", script)
                self.assertIn(b"internal gaps break chart lines", script)
                self.assertIn(b"segments.push(segment)", script)
                self.assertIn(b"for(const points of segments)", script)
                self.assertIn(b"function mountChartBuilder", script)
                self.assertIn(b"function mountLocalYdbProfile", script)
                self.assertIn(b"local-load-allow-errors", script)
                self.assertIn(b"allow-errors: ", script)
                self.assertIn(b"Failed workload requests are allowed", script)
                self.assertIn(b"function localElapsed(started,finished=null)", script)
                self.assertIn(b"Search process", script)
                self.assertIn(b"Ternary search progress", script)
                self.assertIn(b"Plateau candidate", script)
                self.assertIn(b"Search low", script)
                self.assertIn(b"Search high", script)
                self.assertNotIn(b"Candidate and current best", script)
                self.assertNotIn(b"label:'Passed'", script)
                self.assertIn(b"function localSearchAxisLabel", script)
                self.assertIn(b"data-local-chart-x", script)
                self.assertIn(b"Attempts (search order)", script)
                self.assertIn(b"container.dataset.localYdbXAxis", script)
                self.assertIn(b"sort((left,right)=>left-right)", script)
                self.assertIn(b"stages.length>1", script)
                self.assertIn(b"chartBinding.xName", script)
                self.assertIn(b"Ternary resolution (%)", script)
                self.assertIn(b"Growth multiplier", script)
                self.assertIn(b"Geometry stages", script)
                self.assertIn(b"Current phase", script)
                self.assertIn(b"Running command", script)
                self.assertIn(b"function localShellArg", script)
                self.assertIn(b"function localCommandDetails", script)
                self.assertIn(b"progress.current_command", script)
                self.assertIn(b"<th>Commands</th>", script)
                self.assertIn(b"class=local-attempts-scroll", script)
                self.assertIn(b"data-local-profile-config", script)
                self.assertIn(b"profileConfigOpen", script)
                self.assertIn(b"role_affinity", script)
                self.assertIn(b"Launch parameters", script)
                self.assertIn(b"local-ydb-profile?profile=", script)
                self.assertIn(b"function defaultActorCharts", script)
                self.assertIn(b"function defaultMemoryCharts", script)
                self.assertIn(b"Local YDB baseline comparison", script)
                self.assertIn(b"function mountLocalYdbComparison", script)
                self.assertIn(b"function mountLocalYdbComparisonCurves", script)
                self.assertIn(b"function localComparisonSemantic", script)
                self.assertIn(b"function localComparisonKey", script)
                self.assertIn(b"Incompatible", script)
                self.assertIn(b"reference===0", script)
                self.assertIn(b"value===null", script)
                self.assertIn(b"Load values", script)
                self.assertIn(b"...Object.keys(config)", script)
                self.assertIn(b"series.benchmark!=='local-ydb'", script)
                self.assertIn(b"median_throughput", script)
                self.assertIn(b"median_dynamic_cpu_mean", script)
                self.assertIn(b"max_errors", script)
                self.assertIn(b"dynamicNodes", script)
                self.assertIn(b"connectMeasuredPoints", script)
                self.assertIn(b"item.rows.has(String(x))", script)
                self.assertIn(b"no values are synthesized", script)
                self.assertIn(b"loadChartData(value.selected,'local-ydb')", script)
                self.assertIn(b"Promise.allSettled", script)
                self.assertIn(b"function defaultChartScope", script)
                self.assertIn(b"['actorPairs','in_flight']", script)
                self.assertIn(b"['actorPairs','star_multiply']", script)
                self.assertIn(b"metric:'median_msgs_per_sec'", script)
                self.assertIn(b"chartTitle=scope.title", script)
                self.assertIn(b"title:facets.map", script)
                self.assertIn(b"worker_max_min_spread_pct", script)
                self.assertIn(b"worker_mean_min_gap_pct", script)
                self.assertIn(b"function mountSingleChart", script)
                self.assertIn(b"function chartMultiplierDimensions", script)
                self.assertIn(b"function labelExpandedSeries", script)
                self.assertIn(b"queried.has(name)", script)
                self.assertIn(b"varyingFacets", script)
                self.assertIn(b"matched.map(item=>item.facets", script)
                self.assertIn(b"indexed.length===1", script)
                self.assertIn(b"singleProfile:true", script)
                self.assertIn(b"{...chart,id:nextId++}", script)
                self.assertIn(b"function expandedSeries", script)
                self.assertIn(b"Add line row", script)
                self.assertIn(b"class=query-row", script)
                self.assertIn(b"Add chart", script)
                self.assertIn(b"function globLabelMatch", script)
                self.assertIn(b"25|50|75", script)
                self.assertIn(b"data-metric=\"combined\"", script)
                self.assertIn(b"Configure chart", script)
                self.assertIn(b"class=modal-backdrop", script)
                self.assertIn(b"role=dialog", script)
                self.assertIn(b"function bindChartTooltips", script)
                self.assertIn(b"function chartNumber", script)
                self.assertIn(b"synchronize=false", script)
                self.assertIn(b"targets=synchronize?panels:[active]", script)
                self.assertIn(b"data-selected-x", script)
                self.assertIn(b"chartBinding.series", script)
                self.assertIn(b'visibility="hidden"', script)
                self.assertNotIn(b"visibility=hidden/>", script)
                self.assertIn(b"rightItem.value-leftItem.value", script)
                self.assertIn(b'chart-color-', script)
                self.assertNotIn(b'<span style="color:', script)
                self.assertIn(b'CPUs: not recorded', script)
            with urllib.request.urlopen(base + "/app.css") as response:
                stylesheet = response.read()
                self.assertIn(b".local-attempts-scroll{max-width:100%;overflow-x:auto}", stylesheet)
                self.assertIn(b".local-attempts{width:max-content;min-width:100%}", stylesheet)
                self.assertNotIn(b".local-attempts{display:block", stylesheet)
            with urllib.request.urlopen(base + "/api/runs") as response:
                self.assertEqual(json.loads(response.read())[0]["id"], "complete")
            with urllib.request.urlopen(base + "/api/local-ydb-comparison?run=complete") as response:
                self.assertEqual(json.loads(response.read()), {"entries": []})
            with self.assertRaises(HTTPError) as context:
                urllib.request.urlopen(base + "/api/local-ydb-comparison?run=missing")
            self.assertEqual(context.exception.code, 400)
            self.assertIn("run not found", json.loads(context.exception.read())["error"])
            request = urllib.request.Request(base + "/api/import", data=self._portable_archive(), method="POST")
            with urllib.request.urlopen(request) as response:
                self.assertEqual(json.loads(response.read())["source"], "imported")
            with self.assertRaisesRegex(Exception, "HTTP Error 400"):
                urllib.request.urlopen(urllib.request.Request(base + "/api/runs", method="POST"))
        finally:
            server.shutdown()
            worker.join()
            server.server_close()

    def test_web_ui_api_exposes_builder_topology_downloads_and_drafts(self):
        self._manifest(self.root / "complete")
        server = make_server("127.0.0.1", 0, self.root)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        base = "http://127.0.0.1:{}".format(server.server_port)
        yaml_text = "ping-bench:\n  ui: {threads: [1], duration: 1, repetitions: 1, affinity: [none]}\n"
        try:

            def json_request(path, body):
                request = urllib.request.Request(
                    base + path,
                    data=json.dumps(body).encode(),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                return json.loads(urllib.request.urlopen(request).read())

            editor = json_request("/api/editor-config", {"yaml": yaml_text, "perf": False})
            self.assertEqual(editor["profiles"][0]["name"], "ui")
            empty_editor = json_request("/api/editor-config", {"yaml": "\n  \n", "perf": False})
            self.assertEqual(empty_editor["profiles"], [])
            self.assertIn("memory-bandwidth-bench", [item["name"] for item in empty_editor["benchmarks"]])
            self.assertIsNone(editor["profiles"][0]["timeout"])
            explicit_timeout = json_request(
                "/api/editor-config",
                {"yaml": yaml_text.replace("affinity: [none]}", "affinity: [none], timeout: 42}"), "perf": False},
            )
            self.assertEqual(explicit_timeout["profiles"][0]["timeout"], 42)
            self.assertIn("ping-bench", [item["name"] for item in editor["benchmarks"]])
            topology = json.loads(urllib.request.urlopen(base + "/api/system-topology").read())
            self.assertIn("allowed_cpus", topology["topology"])
            self.assertEqual(len(topology["affinity"]), 12)
            draft = json_request("/api/drafts", {"yaml": yaml_text})
            self.assertTrue(Path(draft["path"]).is_file())
            with urllib.request.urlopen(base + "/api/runs/complete/config") as response:
                self.assertIn("attachment", response.headers["Content-Disposition"])
                self.assertIn(b"ping-bench", response.read())
            with urllib.request.urlopen(base + "/api/runs/complete/config.json") as response:
                self.assertIn("ping-bench", json.loads(response.read())["yaml"])
            with urllib.request.urlopen(base + "/api/runs/complete/archive") as response:
                self.assertEqual(response.headers["Content-Type"], "application/zip")
                self.assertTrue(response.read().startswith(b"PK"))
            with urllib.request.urlopen(base + "/api/runs/complete/artifact/artifact.txt") as response:
                self.assertEqual(response.read(), b"artifact")
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

    def test_web_uses_ipv6_socket_for_ipv6_listener(self):
        server = make_server("::1", 0, self.root, allow_remote=True)
        try:
            self.assertEqual(server.address_family, socket.AF_INET6)
        finally:
            server.server_close()

    def test_web_run_api_validates_plans_runs_reconnects_and_cancels(self):
        """The web service owns a fake executor after each HTTP request ends."""
        started = threading.Event()
        release = threading.Event()

        def fake_executor(run, emit, cancelled):
            step = run["store"].manifest["steps"][0]
            emit({"type": "step-started", "step_id": step["id"]})
            emit(
                {
                    "type": "step-progress",
                    "step_id": step["id"],
                    "fields": {"progress": {"phase": "measuring", "attempt": 2}},
                }
            )
            emit({"type": "stdout", "data": "fake output\\n"})
            started.set()
            while not release.wait(0.01):
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
                value = urllib.request.urlopen(
                    urllib.request.Request(base + path, data=body, method=method), timeout=3
                ).read()
                return json.loads(value)

            self.assertTrue(request("/api/validate", "POST", yaml_text.encode())["valid"])
            self.assertEqual(len(request("/api/plan", "POST", yaml_text.encode())["plan"]), 1)
            created = request("/api/runs", "POST", yaml_text.encode())
            self.assertTrue(started.wait(2))
            detail = request("/api/runs/" + created["id"])
            self.assertEqual(detail["steps"][0]["state"], "running")
            self.assertEqual(detail["steps"][0]["progress"], {"phase": "measuring", "attempt": 2})
            self.assertIn("fake output", detail["tail"]["stdout"])
            self.assertIn(
                "step-started", urllib.request.urlopen(base + "/api/runs/" + created["id"] + "/events").read().decode()
            )
            self.assertTrue(request("/api/runs/" + created["id"] + "/cancel", "POST")["cancelled"])
            self.assertTrue(request("/api/runs/" + created["id"] + "/cancel", "POST")["cancelled"])
            for _ in range(100):
                if request("/api/runs/" + created["id"])["state"] == "cancelled":
                    break
                time.sleep(0.01)
            self.assertEqual(request("/api/runs/" + created["id"])["state"], "cancelled")
        finally:
            release.set()
            server.shutdown()
            server.server_close()

    def test_run_service_serializes_emission_and_idempotent_cancellation(self):
        """Executor progress and duplicate cancel requests share one ordered publication boundary."""
        race = threading.Barrier(3)
        release_executor = threading.Event()

        def fake_executor(run, emit, _cancelled):
            step_id = run["store"].manifest["steps"][0]["id"]
            emit({"type": "step-started", "step_id": step_id})
            race.wait()
            emit({"type": "stdout", "data": "executor progress\n"})
            emit({"type": "step-finished", "step_id": step_id, "state": "passed"})
            self.assertTrue(release_executor.wait(2))

        service = RunService(self.root, executor=fake_executor)
        yaml_text = "ping-bench:\n  race: {threads: [1], duration: 1, repetitions: 1, affinity: [none]}\n"
        run_id = service.start(yaml_text)["id"]
        responses = []

        def cancel():
            race.wait()
            responses.append(service.cancel(run_id))

        cancellers = [threading.Thread(target=cancel) for _ in range(2)]
        for thread in cancellers:
            thread.start()
        for thread in cancellers:
            thread.join(2)
            self.assertFalse(thread.is_alive())
        release_executor.set()

        run = service._runs[run_id]
        self.assertTrue(run["finished"].wait(2))
        events = service.events(run_id)
        self.assertEqual([event["sequence"] for event in events], list(range(1, len(events) + 1)))
        self.assertEqual(sum(event["type"] == "cancel-requested" for event in events), 1)
        self.assertEqual(events[-1]["type"], "run-finished")
        self.assertEqual(events[-1]["state"], "cancelled")
        self.assertEqual(len(responses), 2)
        self.assertTrue(all(response["cancelled"] for response in responses))
        self.assertEqual(run["store"].manifest["events"], len(events))
        persisted = [json.loads(line) for line in (run["root"] / "events.jsonl").read_text().splitlines()]
        self.assertEqual(persisted, events)

        # Cancelling a terminal run is also idempotent and must not append an event.
        self.assertEqual(service.cancel(run_id)["state"], "cancelled")
        self.assertEqual(service.events(run_id), events)

    def test_run_service_shutdown_cancels_and_joins_active_workers(self):
        """Server teardown cancels the active run and every queued run."""
        started = threading.Event()
        cancellation_seen = threading.Event()
        queued_started = threading.Event()

        def fake_executor(run, emit, cancelled):
            if run["loaded"].runs[0].profile == "queued":
                queued_started.set()
                return
            step_id = run["store"].manifest["steps"][0]["id"]
            emit({"type": "step-started", "step_id": step_id})
            started.set()
            self.assertTrue(cancelled.wait(2))
            cancellation_seen.set()

        server = make_server("127.0.0.1", 0, self.root, executor=fake_executor)
        yaml_text = "ping-bench:\n  shutdown: {threads: [1], duration: 1, repetitions: 1, affinity: [none]}\n"
        run_id = server.service.start(yaml_text)["id"]
        self.assertTrue(started.wait(2))
        queued_yaml = "ping-bench:\n  queued: {threads: [1], duration: 1, repetitions: 1, affinity: [none]}\n"
        queued_id = server.service.start(queued_yaml)["id"]
        dispatcher = server.service._dispatcher_thread

        # Both public HTTP-server teardown paths are lifecycle boundaries.  A
        # direct close is enough even when serve_forever was never entered.
        server.server_close()
        self.assertTrue(cancellation_seen.is_set())
        self.assertFalse(queued_started.is_set())
        run = server.service._runs[run_id]
        self.assertFalse(dispatcher.is_alive())
        manifest = load_manifest(run["root"] / "run.json")
        self.assertEqual(manifest["state"], "cancelled")
        self.assertEqual(manifest["status"], "cancelled")
        self.assertIn("finished_at", manifest)
        self.assertTrue(all(step["state"] == "cancelled" for step in manifest["steps"]))
        events = server.service.events(run_id)
        self.assertEqual(sum(event["type"] == "cancel-requested" for event in events), 1)
        self.assertEqual(events[-1]["type"], "run-finished")
        queued_manifest = load_manifest(server.service._runs[queued_id]["root"] / "run.json")
        self.assertEqual(queued_manifest["state"], "cancelled")
        self.assertNotIn("started_at", queued_manifest)
        self.assertTrue(all(step["state"] == "cancelled" for step in queued_manifest["steps"]))

        # Repeated shutdown is a no-op, and no run can cross the closed
        # service's start-vs-shutdown publication boundary.
        self.assertEqual(server.service.shutdown(), server.service.shutdown())
        with self.assertRaisesRegex(BenchmarkError, "shutting down"):
            server.service.start(yaml_text)

    def test_run_service_shutdown_timeout_does_not_claim_a_live_worker_is_terminal(self):
        """A diagnostic timeout reports unfinished work without hiding an orphan."""
        started = threading.Event()
        release = threading.Event()

        def stubborn_executor(run, emit, _cancelled):
            step_id = run["store"].manifest["steps"][0]["id"]
            emit({"type": "step-started", "step_id": step_id})
            started.set()
            release.wait(2)
            emit({"type": "step-finished", "step_id": step_id, "state": "passed"})

        service = RunService(self.root, executor=stubborn_executor)
        yaml_text = "ping-bench:\n  stubborn: {threads: [1], duration: 1, repetitions: 1, affinity: [none]}\n"
        run_id = service.start(yaml_text)["id"]
        self.assertTrue(started.wait(2))
        dispatcher = service._dispatcher_thread
        before = time.monotonic()
        result = service.shutdown(timeout=0.05)
        elapsed = time.monotonic() - before
        try:
            self.assertLess(elapsed, 0.5)
            self.assertEqual(result["timed_out"], [run_id])
            manifest_path = service._runs[run_id]["root"] / "run.json"
            incomplete_manifest = load_manifest(manifest_path)
            self.assertEqual(incomplete_manifest["state"], "running")
            self.assertNotIn("finished_at", incomplete_manifest)
            incomplete_events = service.events(run_id)
            self.assertEqual(incomplete_events[-1]["type"], "cancel-requested")
            self.assertTrue(dispatcher.is_alive())
        finally:
            release.set()
        # A later production-style shutdown must continue waiting rather than
        # returning a cached incomplete result.
        completed = service.shutdown()
        self.assertEqual(completed["timed_out"], [])
        self.assertFalse(dispatcher.is_alive())
        terminal_manifest = load_manifest(manifest_path)
        self.assertEqual(terminal_manifest["state"], "cancelled")
        self.assertIn("finished_at", terminal_manifest)
        terminal_events = service.events(run_id)
        self.assertEqual(terminal_events[-1]["type"], "run-finished")
        self.assertEqual(sum(event["type"] == "cancel-requested" for event in terminal_events), 1)

    def test_run_service_rejects_a_start_racing_with_shutdown(self):
        """A run delayed before publication is rejected after shutdown wins the race."""
        topology = discover_topology()
        discovering = threading.Event()
        release = threading.Event()
        errors = []

        def delayed_topology():
            discovering.set()
            self.assertTrue(release.wait(2))
            return topology

        service = RunService(self.root, executor=lambda *_args: None)
        yaml_text = "ping-bench:\n  race: {threads: [1], duration: 1, repetitions: 1, affinity: [none]}\n"

        def start():
            try:
                service.start(yaml_text)
            except Exception as error:
                errors.append(error)

        with mock.patch.object(web, "discover_topology", side_effect=delayed_topology):
            starter = threading.Thread(target=start)
            starter.start()
            self.assertTrue(discovering.wait(2))
            self.assertEqual(service.shutdown(timeout=0.1)["cancelled"], [])
            release.set()
            starter.join(2)
        self.assertFalse(starter.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], BenchmarkError)
        self.assertIn("shutting down", str(errors[0]))
        self.assertEqual(service._runs, {})
        self.assertEqual(list(self.root.glob("*-web")), [])

    def test_run_service_executes_web_runs_in_fifo_order_and_cancels_queued_run(self):
        """Only the dispatcher may promote a queued run into the executor."""
        started = {name: threading.Event() for name in ("first", "second", "third")}
        release = {name: threading.Event() for name in ("first", "second")}
        execution_order = []

        def fake_executor(run, emit, cancelled):
            profile = run["loaded"].runs[0].profile
            execution_order.append(profile)
            started[profile].set()
            self.assertTrue(release[profile].wait(2))
            if cancelled.is_set():
                return
            step_id = run["store"].manifest["steps"][0]["id"]
            emit({"type": "step-started", "step_id": step_id})
            emit({"type": "step-finished", "step_id": step_id, "state": "passed"})

        def config(profile):
            return "ping-bench:\n  {}: {{threads: [1], duration: 1, repetitions: 1, affinity: [none]}}\n".format(
                profile
            )

        service = RunService(self.root, executor=fake_executor)
        first = service.start(config("first"))
        self.assertEqual(first["state"], "queued")
        self.assertTrue(started["first"].wait(2))
        second = service.start(config("second"))
        third = service.start(config("third"))

        second_detail = service.detail(second["id"])
        third_detail = service.detail(third["id"])
        self.assertEqual(second_detail["state"], "queued")
        self.assertEqual(second_detail["queue_position"], 1)
        self.assertEqual(second_detail["current_run_id"], first["id"])
        self.assertEqual(third_detail["queue_position"], 2)
        self.assertTrue(all(step["state"] == "pending" for step in second_detail["steps"]))

        cancelled = service.cancel(third["id"])
        self.assertEqual(cancelled["state"], "cancelled")
        self.assertTrue(service._runs[third["id"]]["finished"].is_set())
        self.assertFalse(started["third"].is_set())
        cancelled_detail = service.detail(third["id"])
        self.assertIsNone(cancelled_detail["started_at"])
        self.assertTrue(all(step["state"] == "cancelled" for step in cancelled_detail["steps"]))
        self.assertEqual(
            [event["type"] for event in service.events(third["id"])],
            ["cancel-requested", "run-finished"],
        )

        release["first"].set()
        self.assertTrue(started["second"].wait(2))
        self.assertEqual(execution_order, ["first", "second"])
        self.assertFalse(started["third"].is_set())
        release["second"].set()
        self.assertTrue(service._runs[second["id"]]["finished"].wait(2))
        self.assertEqual(service.detail(first["id"])["state"], "passed")
        self.assertEqual(service.detail(second["id"])["state"], "passed")
        self.assertEqual(execution_order, ["first", "second"])

    def test_failed_web_run_does_not_block_next_queued_run(self):
        """A run-level failure is terminal for that run, not for the web FIFO."""
        first_started = threading.Event()
        release_failure = threading.Event()
        second_finished = threading.Event()

        def fake_executor(run, emit, _cancelled):
            profile = run["loaded"].runs[0].profile
            if profile == "fail":
                first_started.set()
                self.assertTrue(release_failure.wait(2))
                raise BenchmarkError("expected failure")
            step_id = run["store"].manifest["steps"][0]["id"]
            emit({"type": "step-started", "step_id": step_id})
            emit({"type": "step-finished", "step_id": step_id, "state": "passed"})
            second_finished.set()

        def config(profile):
            return "ping-bench:\n  {}: {{threads: [1], duration: 1, repetitions: 1, affinity: [none]}}\n".format(
                profile
            )

        service = RunService(self.root, executor=fake_executor)
        failed = service.start(config("fail"), continue_on_error=True)
        self.assertTrue(first_started.wait(2))
        passed = service.start(config("pass"), continue_on_error=False)
        self.assertEqual(service.detail(passed["id"])["state"], "queued")
        release_failure.set()
        self.assertTrue(second_finished.wait(2))
        self.assertTrue(service._runs[passed["id"]]["finished"].wait(2))
        self.assertEqual(service.detail(failed["id"])["state"], "failed")
        self.assertEqual(service.detail(passed["id"])["state"], "passed")


if __name__ == "__main__":
    unittest.main()
