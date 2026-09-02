import csv
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
import subprocess
import tempfile
import textwrap
import threading
import time
import unittest
import urllib.request
import zipfile
from collections import deque
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
    local_ydb_workloads,
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

    def _run_mock_local_ydb(
        self,
        configuration,
        output_name,
        measurements,
        cancel_event=None,
        cleanup_action=None,
        cpu_metrics=None,
        lifecycle_actions=None,
    ):
        def record(action, command=None):
            if lifecycle_actions is None:
                return
            path = None
            if command is not None and "--path" in command:
                path = str(command[command.index("--path") + 1])
            lifecycle_actions.append((action, path))

        def command_result(command, stdout="", exit_code=0, stderr="", interrupted=False, timed_out=False):
            return runner.CommandResult(
                command=tuple(str(part) for part in command),
                stdout=stdout,
                stderr=stderr,
                exit_code=exit_code,
                started_at="2026-08-25T10:00:00+00:00",
                finished_at="2026-08-25T10:00:01+00:00",
                duration_seconds=1.0,
                interrupted=interrupted,
                timed_out=timed_out,
            )

        def measurement_stdout(value):
            if isinstance(value, BaseException):
                raise value
            return """
                Total Txs Txs/Sec Retries Errors p50(ms) p95(ms) p99(ms) pMax(ms)
                {transactions} {throughput} 0 {errors} 1 2 {p99_ms} 4
            """.format(
                transactions=value.get("transactions", 1),
                throughput=value.get("throughput", 10),
                errors=value.get("errors", 0),
                p99_ms=value.get("p99_ms", 3),
            )

        cluster = mock.Mock(
            ydb_cli=Path("/tmp/ydb"),
            client_endpoint="grpc://benchmark-host:2135",
            database="/Root/bench",
            dynamic_nodes=[{}],
            static_pids=(10,),
            dynamic_pids=(20,),
        )

        def init_workload(command, **_kwargs):
            record("init", command)
            return command_result(command), [command_result(command)]

        cluster.init_workload.side_effect = init_workload

        def add_dynamic_nodes(count):
            record("scale")
            cluster.dynamic_nodes.extend({} for _index in range(count))

        cluster.add_dynamic_nodes.side_effect = add_dynamic_nodes

        def run_cluster_command(command, **_kwargs):
            if cleanup_action is not None and "clean" in command:
                cleanup_action()
            if "clean" in command:
                record("clean", command)
            return command_result(command)

        cluster._run.side_effect = run_cluster_command
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
        if cpu_metrics is not None:
            monitor.stop.return_value.update(cpu_metrics)
        monitor.summary.return_value = {
            "static_cpu_mean": 11,
            "static_cpu_max": 12,
            "dynamic_cpu_mean": 13,
            "dynamic_cpu_max": 14,
            "cli_cpu_mean": 15,
            "cli_cpu_max": 16,
            "host_cpu_mean": 17,
            "host_cpu_max": 18,
        }
        outputs = iter(measurements)

        def next_measurement(command):
            record("measure", command)
            value = next(outputs)
            stdout = measurement_stdout(value)
            return command_result(
                command,
                stdout,
                exit_code=value.get("exit_code", 0),
                stderr=value.get("stderr", ""),
                interrupted=value.get("interrupted", False),
                timed_out=value.get("timed_out", False),
            )

        output = self.root / output_name
        binaries = {
            name: mock.Mock(path=self.root / name, sha256=name + "-digest", size=1)
            for name in ("ydbd", "ydb_cli", "process_guard")
        }
        events = []
        self.last_local_ydb_cluster = cluster
        self.last_local_ydb_monitor = monitor
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
            side_effect=lambda command, *_args, **_kwargs: next_measurement(command),
        ):
            manifest = local_ydb.run_local_ydb(
                binaries,
                configuration,
                output,
                tool_revision="test",
                event_sink=events.append,
                cancel_event=cancel_event,
            )
        return manifest, output, events

    @staticmethod
    def _local_ydb_command_result(command, stdout="", exit_code=0, interrupted=False):
        return runner.CommandResult(
            command=tuple(str(part) for part in command),
            stdout=stdout,
            stderr="",
            exit_code=exit_code,
            started_at="2026-08-25T10:00:00+00:00",
            finished_at="2026-08-25T10:00:01+00:00",
            duration_seconds=1.0,
            interrupted=interrupted,
        )

    @staticmethod
    def _tpcc_result_payload(
        warehouses=2,
        max_sessions=20,
        threads=2,
        warmup_seconds=2,
        time_seconds=12,
        measure_start_ts=1000,
        new_orders=120,
        latency_transaction="Payment",
        selected_ok=300,
    ):
        full_percentile_values = {"50": 101, "90": 102, "95": 103, "99": 104, "99.9": 105}
        admitted_percentile_values = {"50": 11, "90": 12, "95": 13, "99": 14, "99.9": 15}
        pure_percentile_values = {"50": 1, "90": 2, "95": 3, "99": 4, "99.9": 5}
        transactions = {}
        for index, name in enumerate(("NewOrder", "Delivery", "OrderStatus", "Payment", "StockLevel")):
            ok_count = new_orders if name == "NewOrder" else 100 + index
            if name == latency_transaction:
                ok_count = selected_ok
            transactions[name] = {
                "ok_count": ok_count,
                "failed_count": index,
                "percentiles": dict(full_percentile_values),
                "percentiles_ms": dict(admitted_percentile_values),
                "percentiles_pure": dict(pure_percentile_values),
            }
        return {
            "summary": {
                "name": "Total",
                "time_seconds": time_seconds,
                "measure_start_ts": measure_start_ts,
                "warehouses": warehouses,
                "new_orders": transactions["NewOrder"]["ok_count"],
                "tpmc": 25.5,
                "efficiency": 80.25,
                "max_sessions": max_sessions,
                "threads": threads,
                "warmup_seconds": warmup_seconds,
            },
            "transactions": transactions,
        }

    @staticmethod
    def _synthetic_profile_workload(
        cleanup_names=("clean",),
        measurement_window=(101.0, 109.0),
        dataset_scope="profile",
    ):
        def prepare(cli_context, _definition, path, _workload):
            return (
                local_ydb_workloads.WorkloadCommandPlan(
                    "init",
                    (cli_context.executable, "synthetic", "init", path),
                    10,
                ),
                local_ydb_workloads.WorkloadCommandPlan(
                    "import",
                    (cli_context.executable, "synthetic", "import", path),
                    20,
                ),
            )

        def run(cli_context, _definition, path, _workload, _parameter, load, seconds, threads, warmup):
            return local_ydb_workloads.WorkloadCommandPlan(
                "run",
                (
                    cli_context.executable,
                    "synthetic",
                    "run",
                    path,
                    "--load",
                    load,
                    "--seconds",
                    seconds,
                    "--threads",
                    threads,
                    "--warmup",
                    warmup,
                ),
                warmup + seconds + 30,
                measurement_window_builder=lambda _stdout: measurement_window,
            )

        def cleanup(cli_context, _definition, path, _workload):
            return tuple(
                local_ydb_workloads.WorkloadCommandPlan(
                    name,
                    (cli_context.executable, "synthetic", name, path),
                    5,
                )
                for name in cleanup_names
            )

        definition = local_ydb_workloads.WorkloadDefinition(
            name="synthetic",
            default_operation="run",
            operations=("run",),
            load_parameters=("sessions",),
            options=(),
            uses_path=True,
            table_name=None,
            init_builder=lambda *_args: (),
            run_builder=lambda *_args: (),
            options_validator=lambda *_args: None,
            dataset_scope=dataset_scope,
            warmup_mode="inline",
            prepare_plan_builder=prepare,
            run_plan_builder=run,
            cleanup_plan_builder=cleanup,
        )
        return definition, {"type": "synthetic", "operation": "run", "options": {}}

    @staticmethod
    def _synthetic_workload_lifecycle(
        workload,
        cluster,
        progress,
        cancel_event=None,
        command_timeout_seconds=None,
    ):
        topology = CpuTopology(
            allowed_cpus=(0,),
            numa_nodes=((0, (0,)),),
            chiplets=(),
            physical_cores=((0,),),
        )
        return local_ydb.WorkloadLifecycle(
            cluster,
            local_ydb_workloads.WorkloadCli(cluster.ydb_cli, cluster.client_endpoint, cluster.database),
            workload,
            {"parameter": "sessions"},
            {"warmup": 5, "duration": 10},
            4,
            LOCAL_YDB_BENCHMARK,
            topology,
            {"ydb_cli": None, "static_nodes": None, "dynamic_nodes": None},
            cancel_event,
            lambda phase, **fields: progress.append({"phase": phase, **fields}),
            command_timeout_seconds=command_timeout_seconds,
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

    def test_local_ydb_workload_registry_generates_schema_and_web_catalog(self):
        workload_schema = CONFIG_SCHEMA["properties"]["local-ydb"]["additionalProperties"]["properties"]["workload"]
        self.assertEqual(workload_schema, local_ydb_workloads.workload_config_schema())

        catalog = local_ydb_workloads.web_workload_catalog()
        self.assertEqual([item["type"] for item in catalog], ["kv", "stock", "log", "tpcc", "topic"])
        self.assertEqual(catalog[0]["operations"], ["upsert", "select", "read-rows", "mixed"])
        self.assertEqual(catalog[0]["load_parameters"], ["rate", "threads"])
        self.assertEqual(catalog[1]["default_operation"], "put-rand-order")
        self.assertEqual(catalog[2]["default_operation"], "bulk-upsert")
        self.assertEqual(catalog[2]["operations"], ["insert", "upsert", "bulk-upsert"])
        self.assertEqual(catalog[2]["load_parameters"], ["threads"])
        self.assertEqual(catalog[2]["options"][4]["choices"], ["row", "column"])
        self.assertEqual(
            [item["result_schema_id"] for item in catalog],
            ["generic-total-v1"] * 3 + ["tpcc-json-v3", "topic-window-v1"],
        )
        self.assertEqual(
            [item["throughput_unit"] for item in catalog],
            ["requests/s", "transactions/s", "batches/s", "new orders/s", "messages/s"],
        )
        self.assertTrue(all(item["reports_errors"] for item in catalog[:4]))
        self.assertFalse(catalog[4]["reports_errors"])
        self.assertEqual(catalog[0]["slo_metrics"]["p99"], "p99_ms")
        self.assertEqual(catalog[3]["load_parameters"], ["max-sessions"])
        self.assertEqual(catalog[3]["slo_metrics"]["p999"], "p999_ms")
        self.assertEqual(catalog[3]["default_client_threads"], 2)
        self.assertIsNone(catalog[3]["default_warmup_seconds"])
        self.assertEqual(catalog[3]["minimum_duration_seconds"], 2)
        self.assertEqual(
            catalog[3]["load_limits"],
            {"max-sessions": {"option": "warehouses", "multiplier": 10}},
        )
        self.assertEqual(catalog[4]["operations"], ["full"])
        self.assertEqual(catalog[4]["load_parameters"], ["rate"])
        self.assertEqual(catalog[4]["slo_metrics"], {"p99": "full_p99_ms"})
        self.assertEqual(catalog[4]["default_client_threads"], 1)
        self.assertEqual([item["default_warmup_seconds"] for item in catalog[:3] + catalog[4:]], [10] * 4)
        self.assertEqual(catalog[4]["minimum_duration_seconds"], 2)
        self.assertEqual(
            {option["name"]: option["default"] for option in catalog[4]["options"]},
            {"partitions": 128, "consumers": 1, "message-size": 10240, "codec": "raw"},
        )
        catalog_parameters = tuple(
            dict.fromkeys(parameter for definition in catalog for parameter in definition["load_parameters"])
        )
        load_schema = CONFIG_SCHEMA["properties"]["local-ydb"]["additionalProperties"]["properties"]["load"]
        self.assertEqual(tuple(load_schema["properties"]["parameter"]["enum"]), catalog_parameters)
        self.assertEqual(local_ydb_workloads.all_load_parameters(), catalog_parameters)
        self.assertEqual(
            next(option for option in catalog[0]["options"] if option["name"] == "init-upserts")["operation_defaults"],
            {"upsert": 0},
        )
        operation_enum = workload_schema["properties"]["operation"]["enum"]
        expected_operations = list(
            dict.fromkeys(operation for definition in catalog for operation in definition["operations"])
        )
        self.assertEqual(operation_enum, expected_operations)
        self.assertEqual(len(operation_enum), len(set(operation_enum)))
        json.dumps(catalog, allow_nan=False)

    def test_local_ydb_workload_registry_preserves_defaults_and_validation(self):
        upsert = local_ydb_workloads.normalize_workload(
            {"type": "kv", "operation": "upsert"},
            "local-ydb.profile.workload",
        )
        select = local_ydb_workloads.normalize_workload(
            {"type": "kv", "operation": "select"},
            "local-ydb.profile.workload",
        )
        stock = local_ydb_workloads.normalize_workload(
            {"type": "stock", "operation": "put-rand-order"},
            "local-ydb.profile.workload",
        )
        self.assertEqual(upsert["options"]["init-upserts"], 0)
        self.assertEqual(select["options"]["init-upserts"], 1000)
        self.assertEqual(stock["options"]["orders"], 100)

        invalid = (
            ({"type": [], "operation": "upsert"}, r"workload\.type.*must be one of kv, stock, log, tpcc, topic"),
            ({"type": "kv", "operation": "put-rand-order"}, r"workload\.operation.*must be one of upsert"),
            (
                {"type": "stock", "operation": "put-rand-order", "options": {"products": 500001}},
                r"products.*must not exceed 500000",
            ),
            (
                {"type": "stock", "operation": "put-rand-order", "options": {"auto-partition": 2}},
                r"auto-partition.*must be 0 or 1",
            ),
            (
                {"type": "stock", "operation": "put-rand-order", "options": {"max-partitions": 10}},
                r"options.*unknown fields: max-partitions",
            ),
        )
        for workload, message in invalid:
            with self.subTest(workload=workload), self.assertRaisesRegex(BenchmarkError, message):
                local_ydb_workloads.normalize_workload(workload, "local-ydb.profile.workload")

    def test_local_ydb_tpcc_defaults_warmup_and_profile_validation(self):
        loaded = load_config(self._config("""
            local-ydb:
              tpcc-smoke:
                workload: {type: tpcc, operation: run}
                load: {parameter: max-sessions, values: [1, 100]}
                measurement: {warmup: 0, duration: 2, repetitions: 1}
        """))
        profile = loaded.runs[0].parameters["local_ydb"]
        self.assertEqual(
            profile["workload"]["options"],
            {
                "warehouses": 10,
                "import-threads": 0,
                "compact": False,
                "tx-mode": "serializable-rw",
                "latency-transaction": "NewOrder",
                "no-delays": False,
                "highres-histogram": False,
            },
        )
        self.assertEqual(profile["client"]["threads"], 2)
        self.assertEqual(profile["load"]["values"], [1, 100])
        self.assertEqual(local_ydb_workloads.workload_effective_warmup_seconds(profile["workload"], 0), 2)
        self.assertEqual(local_ydb_workloads.workload_effective_warmup_seconds(profile["workload"], 1), 2)
        self.assertEqual(local_ydb_workloads.workload_effective_warmup_seconds(profile["workload"], 7), 7)

        automatic = load_config(
            self._config(
                """
            local-ydb:
              tpcc-automatic:
                workload: {type: tpcc, operation: run}
                load: {parameter: max-sessions, values: [1]}
                measurement: {duration: 2, repetitions: 1}
        """,
                "tpcc-automatic.yaml",
            )
        )
        automatic_profile = automatic.runs[0].parameters["local_ydb"]
        self.assertIsNone(automatic_profile["measurement"]["warmup"])
        self.assertEqual(
            local_ydb_workloads.workload_effective_warmup_seconds(automatic_profile["workload"], None),
            30,
        )
        self.assertEqual(automatic.runs[0].timeout_seconds, 300 + 30 + 2 + 10)

        explicit_null = self._config(
            """
            local-ydb:
              invalid-tpcc-warmup:
                workload: {type: tpcc, operation: run}
                load: {parameter: max-sessions, values: [1]}
                measurement: {warmup: null, duration: 2, repetitions: 1}
        """,
            "tpcc-null-warmup.yaml",
        )
        with self.assertRaisesRegex(BenchmarkError, "warmup"):
            load_config(explicit_null)

    def test_local_ydb_tpcc_adaptive_warmup_matches_cli_heuristic(self):
        for warehouses, expected in (
            (10, 30),
            (11, 300),
            (100, 300),
            (101, 600),
            (1000, 600),
            (1001, 1200),
            (10000, 1200),
            (10001, 1800),
            (20000, 2001),
            (100000, 3600),
        ):
            with self.subTest(warehouses=warehouses):
                workload = local_ydb_workloads.normalize_workload(
                    {"type": "tpcc", "operation": "run", "options": {"warehouses": warehouses}},
                    "workload",
                )
                self.assertEqual(local_ydb_workloads.workload_effective_warmup_seconds(workload, None), expected)

        kv = local_ydb_workloads.normalize_workload({"type": "kv", "operation": "upsert"}, "workload")
        with self.assertRaisesRegex(BenchmarkError, "does not support automatic warmup"):
            local_ydb_workloads.workload_effective_warmup_seconds(kv, None)
        with self.assertRaisesRegex(BenchmarkError, "does not support automatic warmup"):
            local_ydb_workloads.build_run_plan(
                local_ydb_workloads.WorkloadCli("ydb", "grpc://host:2135", "/Root/bench"),
                "table",
                kv,
                "rate",
                1,
                1,
                1,
                warmup_seconds=None,
            )

        invalid_profiles = (
            (
                """
                local-ydb:
                  invalid-tpcc:
                    workload: {type: tpcc, operation: run}
                    load: {parameter: max-sessions, values: [101]}
                """,
                "load.values\\[0\\].*warehouses \\* 10",
            ),
            (
                """
                local-ydb:
                  invalid-tpcc:
                    workload: {type: tpcc, operation: run}
                    load:
                      parameter: max-sessions
                      search: {start: 1, maximum: 101}
                      objective: {type: maximize-throughput}
                """,
                "load.search.maximum.*warehouses \\* 10",
            ),
            (
                """
                local-ydb:
                  invalid-tpcc:
                    workload: {type: tpcc, operation: run}
                    load: {parameter: max-sessions, values: [1]}
                    measurement: {duration: 1}
                """,
                "measurement.duration.*at least 2",
            ),
        )
        for index, (body, message) in enumerate(invalid_profiles):
            with self.subTest(index=index), self.assertRaisesRegex(BenchmarkError, message):
                load_config(self._config(body, "invalid-tpcc-{}.yaml".format(index)))

    def test_local_ydb_topic_defaults_and_profile_validation(self):
        loaded = load_config(self._config("""
            local-ydb:
              topic-smoke:
                workload: {type: topic, operation: full}
                load: {parameter: rate, values: [100]}
                measurement: {warmup: 1, duration: 2, repetitions: 1}
        """))
        profile = loaded.runs[0].parameters["local_ydb"]
        self.assertEqual(
            profile["workload"]["options"],
            {"partitions": 128, "consumers": 1, "message-size": 10240, "codec": "raw"},
        )
        self.assertEqual(profile["client"]["threads"], 1)
        self.assertEqual(profile["load"], {"parameter": "rate", "allow_errors": False, "values": [100]})
        default_warmup = load_config(
            self._config(
                """
            local-ydb:
              topic-default-warmup:
                workload: {type: topic, operation: full}
                load: {parameter: rate, values: [100]}
                measurement: {duration: 2, repetitions: 1}
        """,
                "topic-default-warmup.yaml",
            )
        )
        self.assertEqual(default_warmup.runs[0].parameters["local_ydb"]["measurement"]["warmup"], 10)
        definition = local_ydb_workloads.workload_definition("topic")
        self.assertEqual((definition.dataset_scope, definition.warmup_mode), ("sample", "inline"))

        latency = (
            load_config(
                self._config(
                    """
            local-ydb:
              topic-latency:
                workload: {type: topic, operation: full}
                load:
                  parameter: rate
                  search: {start: 10, maximum: 100, multiplier: 2}
                  objective: {type: latency-slo, percentile: p99, max-ms: 20, max-errors: 0}
                measurement: {duration: 2}
        """,
                    "topic-latency.yaml",
                )
            )
            .runs[0]
            .parameters["local_ydb"]
        )
        self.assertEqual(latency["load"]["objective"]["latency_metric"], "full_p99_ms")

        invalid_profiles = (
            (
                """
                local-ydb:
                  invalid-topic:
                    workload: {type: topic, operation: full}
                    load: {parameter: rate, allow-errors: true, values: [10]}
                    measurement: {duration: 2}
                """,
                "load.allow-errors.*must be false",
            ),
            (
                """
                local-ydb:
                  invalid-topic:
                    workload: {type: topic, operation: full}
                    load:
                      parameter: rate
                      search: {start: 10, maximum: 100, multiplier: 2}
                      objective: {type: latency-slo, percentile: p99, max-ms: 20, max-errors: 1}
                    measurement: {duration: 2}
                """,
                "load.objective.max-errors.*must be zero",
            ),
            (
                """
                local-ydb:
                  invalid-topic:
                    workload: {type: topic, operation: full}
                    load: {parameter: rate, values: [10]}
                    measurement: {duration: 1}
                """,
                "measurement.duration.*at least 2",
            ),
            (
                """
                local-ydb:
                  invalid-topic:
                    workload: {type: topic, operation: full, options: {codec: snappy}}
                    load: {parameter: rate, values: [10]}
                    measurement: {duration: 2}
                """,
                "codec.*must be one of raw, gzip, zstd",
            ),
            (
                """
                local-ydb:
                  invalid-topic:
                    workload: {type: topic, operation: full}
                    load: {parameter: threads, values: [1]}
                    measurement: {duration: 2}
                """,
                "load.parameter.*must be one of rate",
            ),
            (
                """
                local-ydb:
                  invalid-topic:
                    workload: {type: topic, operation: full}
                    load:
                      parameter: rate
                      search: {start: 10, maximum: 100, multiplier: 2}
                      objective: {type: latency-slo, percentile: p95, max-ms: 20}
                    measurement: {duration: 2}
                """,
                "percentile.*must be one of p99",
            ),
        )
        for index, (body, message) in enumerate(invalid_profiles):
            with self.subTest(index=index), self.assertRaisesRegex(BenchmarkError, message):
                load_config(self._config(body, "invalid-topic-{}.yaml".format(index)))

    def test_local_ydb_workload_registry_supports_typed_options(self):
        definition = local_ydb_workloads.WorkloadDefinition(
            name="synthetic",
            default_operation="run",
            operations=("run",),
            load_parameters=("threads",),
            options=(
                local_ydb_workloads.WorkloadOption(
                    "tx-mode",
                    "serializable-rw",
                    kind="string",
                    choices=("serializable-rw", "snapshot-rw"),
                ),
                local_ydb_workloads.WorkloadOption("enabled", True, kind="boolean"),
                local_ydb_workloads.WorkloadOption(
                    "warmup",
                    "10s",
                    kind="duration",
                    pattern=r"^[1-9][0-9]*s$",
                ),
                local_ydb_workloads.WorkloadOption("label", "mode: fast # tagged", kind="string"),
            ),
            uses_path=True,
            table_name=None,
            init_builder=None,
            run_builder=None,
            options_validator=lambda options, location: None,
        )
        local_ydb_workloads._validate_catalog((definition,))
        divergent = replace(
            definition,
            name="other",
            options=(local_ydb_workloads.WorkloadOption("tx-mode", 1),),
        )
        with self.assertRaisesRegex(ValueError, "tx-mode has incompatible schemas"):
            local_ydb_workloads._validate_catalog((definition, divergent))
        for pattern in (r"(?P<seconds>[0-9]+)s", r"^[0-9]+\s+s$"):
            nonportable = replace(
                definition,
                options=(local_ydb_workloads.WorkloadOption("warmup", "10s", kind="duration", pattern=pattern),),
            )
            with self.subTest(pattern=pattern), self.assertRaisesRegex(ValueError, "pattern.*is not portable"):
                local_ydb_workloads._validate_catalog((nonportable,))
        with mock.patch.object(local_ydb_workloads, "_DEFINITIONS", (definition,)), mock.patch.object(
            local_ydb_workloads,
            "_WORKLOADS",
            {definition.name: definition},
        ):
            workload = local_ydb_workloads.normalize_workload(
                {"type": "synthetic", "operation": "run"},
                "workload",
            )
            self.assertEqual(
                workload["options"],
                {
                    "tx-mode": "serializable-rw",
                    "enabled": True,
                    "warmup": "10s",
                    "label": "mode: fast # tagged",
                },
            )
            schema = local_ydb_workloads.workload_config_schema()["properties"]["options"]["properties"]
            self.assertEqual(
                schema["tx-mode"],
                {
                    "type": "string",
                    "minLength": 1,
                    "enum": ["serializable-rw", "snapshot-rw"],
                },
            )
            self.assertEqual(schema["enabled"], {"type": "boolean"})
            self.assertEqual(schema["warmup"]["pattern"], r"^[1-9][0-9]*s$")
            catalog = local_ydb_workloads.web_workload_catalog()
            self.assertEqual(
                [option["kind"] for option in catalog[0]["options"]],
                ["string", "boolean", "duration", "string"],
            )
            self.assertEqual(catalog[0]["options"][2]["schema"], schema["warmup"])
            special = "mode: fast # tagged\nsecond line"
            self.assertEqual(yaml.safe_load("option: " + json.dumps(special))["option"], special)

            invalid = (
                ({"tx-mode": "", "enabled": True, "warmup": "10s"}, "tx-mode.*must not be empty"),
                ({"tx-mode": "snapshot-rw", "enabled": 1, "warmup": "10s"}, "enabled.*must be a boolean"),
                ({"tx-mode": "snapshot-rw", "enabled": False, "warmup": "0s"}, "warmup.*must match pattern"),
            )
            for options, message in invalid:
                with self.subTest(options=options), self.assertRaisesRegex(BenchmarkError, message):
                    local_ydb_workloads.normalize_workload(
                        {"type": "synthetic", "operation": "run", "options": options},
                        "workload",
                    )

    def test_local_ydb_workload_registry_builds_golden_cli_commands(self):
        cli_context = local_ydb_workloads.WorkloadCli(
            Path("/tmp/ydb cli"),
            "grpc://benchmark-host.example:2135",
            "/Root/bench",
        )
        workload = local_ydb_workloads.normalize_workload(
            {"type": "kv", "operation": "select"},
            "workload",
        )
        base = [
            Path("/tmp/ydb cli"),
            "--endpoint",
            "grpc://benchmark-host.example:2135",
            "--database",
            "/Root/bench",
            "workload",
            "kv",
            "--path",
            "table-prefix",
        ]
        self.assertEqual(
            local_ydb_workloads.build_init_argv(cli_context, "table-prefix", workload),
            base
            + [
                "init",
                "--init-upserts",
                1000,
                "--min-partitions",
                40,
                "--max-partitions",
                1000,
                "--partition-size",
                2000,
                "--max-first-key",
                65536,
                "--len",
                64,
                "--cols",
                2,
                "--int-cols",
                1,
                "--key-cols",
                1,
                "--rows",
                1,
            ],
        )
        self.assertEqual(
            local_ydb_workloads.build_run_argv(cli_context, "table-prefix", workload, "rate", 250, 30, 64),
            base
            + [
                "run",
                "select",
                "--seconds",
                30,
                "--threads",
                64,
                "--quiet",
                "--max-first-key",
                65536,
                "--int-cols",
                1,
                "--key-cols",
                1,
                "--cols",
                2,
                "--rows",
                1,
                "--rate",
                250,
            ],
        )
        self.assertEqual(
            local_ydb_workloads.build_clean_argv(cli_context, "table-prefix", "kv"),
            base + ["clean"],
        )
        prepare = local_ydb_workloads.build_prepare_plan(cli_context, "table-prefix", workload)
        run = local_ydb_workloads.build_run_plan(
            cli_context,
            "table-prefix",
            workload,
            "rate",
            250,
            30,
            64,
        )
        cleanup = local_ydb_workloads.build_cleanup_plan(cli_context, "table-prefix", workload)
        self.assertEqual(
            prepare,
            (
                local_ydb_workloads.WorkloadCommandPlan(
                    "init",
                    tuple(
                        base
                        + [
                            "init",
                            "--init-upserts",
                            1000,
                            "--min-partitions",
                            40,
                            "--max-partitions",
                            1000,
                            "--partition-size",
                            2000,
                            "--max-first-key",
                            65536,
                            "--len",
                            64,
                            "--cols",
                            2,
                            "--int-cols",
                            1,
                            "--key-cols",
                            1,
                            "--rows",
                            1,
                        ]
                    ),
                    120,
                ),
            ),
        )
        self.assertEqual(
            run.argv,
            tuple(
                local_ydb_workloads.build_run_argv(
                    cli_context,
                    "table-prefix",
                    workload,
                    "rate",
                    250,
                    30,
                    64,
                )
            ),
        )
        self.assertEqual(run.timeout_seconds, 60)
        self.assertEqual(cleanup[0].argv, tuple(base + ["clean"]))
        self.assertEqual(cleanup[0].timeout_seconds, 120)

    def test_local_ydb_cleanup_plan_rejects_custom_plan_without_timeout(self):
        cli_context = local_ydb_workloads.WorkloadCli(Path("ydb"), "grpc://host:2135", "/Root/bench")

        def unbounded_cleanup(*_args):
            return (local_ydb_workloads.WorkloadCommandPlan("clean", ("ydb", "clean")),)

        definition = replace(
            local_ydb_workloads.workload_definition("kv"),
            cleanup_plan_builder=unbounded_cleanup,
        )
        with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {"kv": definition}):
            with self.assertRaisesRegex(BenchmarkError, "must have a positive timeout"):
                local_ydb_workloads.build_cleanup_plan(cli_context, "table-prefix", {"type": "kv"})

    def test_local_ydb_tpcc_builds_golden_prepare_run_and_cleanup_plans(self):
        cli_context = local_ydb_workloads.WorkloadCli(
            Path("/tmp/ydb cli"),
            "grpc://benchmark-host.example:2135",
            "/Root/bench",
        )
        workload = local_ydb_workloads.normalize_workload(
            {
                "type": "tpcc",
                "operation": "run",
                "options": {
                    "warehouses": 12,
                    "import-threads": 0,
                    "compact": True,
                    "tx-mode": "snapshot-rw",
                    "latency-transaction": "Payment",
                    "no-delays": True,
                    "highres-histogram": True,
                },
            },
            "workload",
        )
        base = [
            Path("/tmp/ydb cli"),
            "--endpoint",
            "grpc://benchmark-host.example:2135",
            "--database",
            "/Root/bench",
            "workload",
            "tpcc",
            "--path",
            "tpcc-dataset",
        ]
        prepare = local_ydb_workloads.build_prepare_plan(cli_context, "tpcc-dataset", workload)
        self.assertEqual(
            prepare,
            (
                local_ydb_workloads.WorkloadCommandPlan(
                    "init",
                    tuple(base + ["init", "--warehouses", 12]),
                    120,
                ),
                local_ydb_workloads.WorkloadCommandPlan(
                    "import",
                    tuple(
                        base
                        + [
                            "import",
                            "--warehouses",
                            12,
                            "--threads",
                            0,
                            "--no-tui",
                            "--compact",
                        ]
                    ),
                    720,
                ),
            ),
        )

        run = local_ydb_workloads.build_run_plan(
            cli_context,
            "tpcc-dataset",
            workload,
            "max-sessions",
            50,
            30,
            3,
            warmup_seconds=0,
        )
        self.assertEqual(
            run.argv,
            tuple(
                base
                + [
                    "run",
                    "--warehouses",
                    12,
                    "--warmup",
                    "2s",
                    "--time",
                    "30s",
                    "--max-sessions",
                    50,
                    "--threads",
                    3,
                    "--format",
                    "Json",
                    "--no-tui",
                    "--tx-mode",
                    "snapshot-rw",
                    "--no-delays",
                    "--highres-histogram",
                ]
            ),
        )
        self.assertEqual(run.timeout_seconds, 92)
        self.assertEqual(run.progress_duration_seconds, 32)
        automatic_warmup = local_ydb_workloads.build_run_plan(
            cli_context,
            "tpcc-dataset",
            workload,
            "max-sessions",
            50,
            30,
            3,
            warmup_seconds=None,
        )
        self.assertNotIn("--warmup", automatic_warmup.argv)
        self.assertEqual(automatic_warmup.timeout_seconds, 390)
        self.assertEqual(automatic_warmup.progress_duration_seconds, 330)
        for invalid_warmup in (-1, True):
            with self.subTest(invalid_warmup=invalid_warmup), self.assertRaisesRegex(
                BenchmarkError, "non-negative integer"
            ):
                local_ydb_workloads.build_run_plan(
                    cli_context,
                    "tpcc-dataset",
                    workload,
                    "max-sessions",
                    50,
                    30,
                    3,
                    warmup_seconds=invalid_warmup,
                )
        requested_warmup = local_ydb_workloads.build_run_plan(
            cli_context,
            "tpcc-dataset",
            workload,
            "max-sessions",
            50,
            30,
            3,
            warmup_seconds=7,
        )
        self.assertEqual(requested_warmup.argv[requested_warmup.argv.index("--warmup") + 1], "7s")
        self.assertEqual(requested_warmup.timeout_seconds, 97)

        cleanup = local_ydb_workloads.build_cleanup_plan(cli_context, "tpcc-dataset", workload)
        self.assertEqual(
            cleanup,
            (
                local_ydb_workloads.WorkloadCommandPlan("clean", tuple(base + ["clean"]), 300),
                local_ydb_workloads.WorkloadCommandPlan(
                    "rmdir",
                    (
                        Path("/tmp/ydb cli"),
                        "--endpoint",
                        "grpc://benchmark-host.example:2135",
                        "--database",
                        "/Root/bench",
                        "scheme",
                        "rmdir",
                        "--recursive",
                        "--force",
                        "/Root/bench/tpcc-dataset",
                    ),
                    300,
                ),
            ),
        )
        absolute_cleanup = local_ydb_workloads.build_cleanup_plan(cli_context, "/Root/other", workload)
        self.assertEqual(absolute_cleanup[1].argv[-1], "/Root/other")

    def test_local_ydb_topic_builds_golden_prepare_run_and_cleanup_plans(self):
        cli_context = local_ydb_workloads.WorkloadCli(
            Path("/tmp/ydb cli"),
            "grpc://benchmark-host.example:2135",
            "/Root/bench",
        )
        workload = local_ydb_workloads.normalize_workload(
            {
                "type": "topic",
                "operation": "full",
                "options": {"partitions": 16, "consumers": 2, "message-size": 4096, "codec": "zstd"},
            },
            "workload",
        )
        base = [
            Path("/tmp/ydb cli"),
            "--endpoint",
            "grpc://benchmark-host.example:2135",
            "--database",
            "/Root/bench",
            "workload",
            "topic",
        ]
        prepare = local_ydb_workloads.build_prepare_plan(cli_context, "topic-sample-01", workload)
        self.assertEqual(
            prepare,
            (
                local_ydb_workloads.WorkloadCommandPlan(
                    "init",
                    tuple(
                        base
                        + [
                            "init",
                            "--topic",
                            "topic-sample-01",
                            "--partitions",
                            16,
                            "--consumers",
                            2,
                            "--consumer-prefix",
                            "ydb-bench-consumer",
                        ]
                    ),
                    120,
                ),
            ),
        )

        direct_run = local_ydb_workloads.build_run_argv(
            cli_context,
            "topic-sample-01",
            workload,
            "rate",
            5000,
            30,
            3,
        )
        expected_run = base + [
            "run",
            "full",
            "--topic",
            "topic-sample-01",
            "--seconds",
            30,
            "--warmup",
            0,
            "--window",
            1,
            "--print-timestamp",
            "--percentile",
            99,
            "--producer-threads",
            3,
            "--consumer-threads",
            3,
            "--consumers",
            2,
            "--consumer-prefix",
            "ydb-bench-consumer",
            "--message-size",
            4096,
            "--message-rate",
            5000,
            "--codec",
            "zstd",
        ]
        self.assertEqual(direct_run, expected_run)
        self.assertNotIn("--use-tx", direct_run)

        run = local_ydb_workloads.build_run_plan(
            cli_context,
            "topic-sample-01",
            workload,
            "rate",
            5000,
            30,
            3,
            warmup_seconds=7,
        )
        expected_run[expected_run.index("--seconds") + 1] = 37
        expected_run[expected_run.index("--warmup") + 1] = 7
        self.assertEqual(run.argv, tuple(expected_run))
        self.assertEqual(run.timeout_seconds, 67)
        self.assertEqual(run.progress_duration_seconds, 37)

        cleanup = local_ydb_workloads.build_cleanup_plan(cli_context, "topic-sample-01", workload)
        self.assertEqual(
            cleanup,
            (
                local_ydb_workloads.WorkloadCommandPlan(
                    "clean",
                    tuple(base + ["clean", "--topic", "topic-sample-01"]),
                    120,
                ),
            ),
        )
        self.assertEqual(local_ydb_workloads.workload_table_path("topic", "topic-sample-01"), "topic-sample-01")

    def test_local_ydb_workload_registry_validates_lifecycle_metadata_and_plans(self):
        definition = local_ydb_workloads.WorkloadDefinition(
            name="inline",
            default_operation="run",
            operations=("run",),
            load_parameters=("sessions",),
            options=(),
            uses_path=True,
            table_name=None,
            init_builder=lambda *_args: (),
            run_builder=lambda *_args: (),
            options_validator=lambda *_args: None,
            dataset_scope="profile",
            warmup_mode="inline",
            run_plan_builder=lambda *_args: local_ydb_workloads.WorkloadCommandPlan("run", ("ydb",), 1),
        )
        local_ydb_workloads._validate_catalog((definition,))
        local_ydb_workloads._validate_catalog((replace(definition, dataset_scope="geometry"),))
        with self.assertRaisesRegex(ValueError, "dataset scope"):
            local_ydb_workloads._validate_catalog((replace(definition, dataset_scope="attempt"),))
        with self.assertRaisesRegex(ValueError, "inline warmup requires"):
            local_ydb_workloads._validate_catalog((replace(definition, run_plan_builder=None),))
        with self.assertRaisesRegex(ValueError, "default client threads"):
            local_ydb_workloads._validate_catalog((replace(definition, default_client_threads=0),))
        for invalid_warmup in (-1, True, "10"):
            with self.subTest(default_warmup_seconds=invalid_warmup), self.assertRaisesRegex(
                ValueError, "default warmup"
            ):
                local_ydb_workloads._validate_catalog((replace(definition, default_warmup_seconds=invalid_warmup),))
        with self.assertRaisesRegex(ValueError, "automatic warmup requires"):
            local_ydb_workloads._validate_catalog(
                (replace(definition, default_warmup_seconds=None, effective_warmup_builder=None),)
            )
        string_limit = replace(
            definition,
            options=(local_ydb_workloads.WorkloadOption("scale", "auto", kind="string"),),
            load_limits=(("sessions", "scale", 10),),
        )
        with self.assertRaisesRegex(ValueError, "invalid load limit"):
            local_ydb_workloads._validate_catalog((string_limit,))
        with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {"inline": definition}):
            with self.assertRaisesRegex(BenchmarkError, "CPU measurement window"):
                local_ydb_workloads.build_run_plan(
                    local_ydb_workloads.WorkloadCli("ydb", "endpoint", "database"),
                    "path",
                    {"type": "inline", "operation": "run", "options": {}},
                    "sessions",
                    1,
                    1,
                    1,
                    warmup_seconds=0,
                )
            with self.assertRaisesRegex(BenchmarkError, "non-empty argv"):
                invalid = replace(
                    definition,
                    run_plan_builder=lambda *_args: local_ydb_workloads.WorkloadCommandPlan("run", (), 1),
                )
                with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {"inline": invalid}):
                    local_ydb_workloads.build_run_plan(
                        local_ydb_workloads.WorkloadCli("ydb", "endpoint", "database"),
                        "path",
                        {"type": "inline", "operation": "run", "options": {}},
                        "sessions",
                        1,
                        1,
                        1,
                        warmup_seconds=1,
                    )
            for timeout in (float("nan"), float("inf"), True, 10**1000):
                nonfinite = replace(
                    definition,
                    run_plan_builder=lambda *_args, timeout=timeout: local_ydb_workloads.WorkloadCommandPlan(
                        "run",
                        ("ydb",),
                        timeout,
                    ),
                )
                with self.subTest(timeout=timeout), mock.patch.object(
                    local_ydb_workloads,
                    "_WORKLOADS",
                    {"inline": nonfinite},
                ), self.assertRaisesRegex(BenchmarkError, "positive timeout"):
                    local_ydb_workloads.build_run_plan(
                        local_ydb_workloads.WorkloadCli("ydb", "endpoint", "database"),
                        "path",
                        {"type": "inline", "operation": "run", "options": {}},
                        "sessions",
                        1,
                        1,
                        1,
                        warmup_seconds=1,
                    )

    def test_local_ydb_tpcc_geometry_lifecycle_reuses_dataset_and_cpu_window(self):
        workload = local_ydb_workloads.normalize_workload(
            {
                "type": "tpcc",
                "operation": "run",
                "options": {
                    "warehouses": 2,
                    "latency-transaction": "Payment",
                },
            },
            "workload",
        )
        cluster = mock.Mock(
            ydb_cli=Path("/tmp/ydb"),
            client_endpoint="grpc://benchmark-host:2135",
            database="/Root/bench",
            static_pids=(10,),
            dynamic_pids=(20,),
        )
        cluster.init_workload.side_effect = lambda command, **_kwargs: (
            self._local_ydb_command_result(command),
            [self._local_ydb_command_result(command)],
        )
        cluster._run.side_effect = lambda command, **_kwargs: self._local_ydb_command_result(command)
        monitor = mock.Mock(records=[])
        monitor.stop.return_value = {}
        monitor.summary.return_value = {
            "static_cpu_mean": 1,
            "static_cpu_max": 2,
            "dynamic_cpu_mean": 3,
            "dynamic_cpu_max": 4,
            "cli_cpu_mean": 5,
            "cli_cpu_max": 6,
            "host_cpu_mean": 7,
            "host_cpu_max": 8,
        }
        payload = self._tpcc_result_payload(
            warehouses=2,
            max_sessions=20,
            threads=2,
            warmup_seconds=30,
            time_seconds=10,
            new_orders=100,
        )
        topology_record = CpuTopology(
            allowed_cpus=(0,),
            numa_nodes=((0, (0,)),),
            chiplets=(),
            physical_cores=((0,),),
        )
        progress = []
        with mock.patch.object(local_ydb, "LinuxCpuMonitor", return_value=monitor), mock.patch.object(
            local_ydb,
            "run_command",
            side_effect=lambda command, *_args, **_kwargs: self._local_ydb_command_result(
                command,
                json.dumps(payload),
            ),
        ) as execute:
            lifecycle = local_ydb.WorkloadLifecycle(
                cluster,
                local_ydb_workloads.WorkloadCli(cluster.ydb_cli, cluster.client_endpoint, cluster.database),
                workload,
                {"parameter": "max-sessions"},
                {"warmup": None, "duration": 10},
                2,
                LOCAL_YDB_BENCHMARK,
                topology_record,
                {"ydb_cli": None, "static_nodes": None, "dynamic_nodes": None},
                None,
                lambda phase, **fields: progress.append({"phase": phase, **fields}),
            )
            lifecycle.open_profile(self.root / "tpcc-profile", "ignored-profile-path")
            lifecycle.open_geometry(self.root / "tpcc-geometry", "tpcc-dataset", 1)
            first_metrics, first_commands = lifecycle.run_sample(
                20,
                1,
                1,
                2,
                self.root / "tpcc-repeat-1",
                "ignored-sample-path-1",
                {"attempt": 1},
            )
            second_metrics, second_commands = lifecycle.run_sample(
                20,
                1,
                2,
                2,
                self.root / "tpcc-repeat-2",
                "ignored-sample-path-2",
                {"attempt": 1},
                purpose="verification",
            )
            lifecycle.close_geometry()
            lifecycle.close_profile()

        self.assertEqual(cluster.init_workload.call_count, 2)
        self.assertEqual(execute.call_count, 2)
        self.assertEqual(cluster._run.call_count, 2)
        self.assertEqual(
            [call.args[0][-1] for call in cluster._run.call_args_list], ["clean", "/Root/bench/tpcc-dataset"]
        )
        self.assertTrue(all(call.kwargs["timeout"] == 300 for call in cluster._run.call_args_list))
        self.assertEqual([command["phase"] for command in first_commands], ["measuring"])
        self.assertEqual([command["phase"] for command in second_commands], ["verification-measuring"])
        run_argv = execute.call_args_list[0].args[0]
        self.assertNotIn("--warmup", run_argv)
        self.assertEqual(run_argv[run_argv.index("--threads") + 1], 2)
        self.assertEqual(first_metrics["throughput"], 10)
        self.assertEqual(second_metrics["transactions"], 300)
        self.assertEqual(monitor.summary.call_count, 2)
        for call in monitor.summary.call_args_list:
            self.assertEqual(call.kwargs, {"started_at_unix": 1001.0, "finished_at_unix": 1010.0})
        details = json.loads((self.root / "tpcc-repeat-1" / "workload-result.json").read_text(encoding="utf-8"))
        self.assertEqual(details["schema_id"], "tpcc-json-v3")
        self.assertEqual(details["details"], payload)
        self.assertEqual(
            [item["phase"] for item in progress],
            [
                "initializing-workload",
                "preparing-import",
                "measuring",
                "verification-measuring",
                "cleaning-workload",
                "cleaning-rmdir",
            ],
        )
        measurements = [item for item in progress if item["phase"].endswith("measuring")]
        self.assertTrue(all(item["inline_warmup_seconds"] == 30 for item in measurements))
        self.assertTrue(all("configured_warmup_seconds" not in item for item in measurements))

    def test_local_ydb_profile_inline_lifecycle_prepares_once_and_cleans_once(self):
        definition, workload = self._synthetic_profile_workload()
        metrics_output = """
            Total Txs Txs/Sec Retries Errors p50(ms) p95(ms) p99(ms) pMax(ms)
            10 10 0 0 1 2 3 4
        """
        cluster = mock.Mock(
            ydb_cli=Path("/tmp/ydb"),
            client_endpoint="grpc://benchmark-host:2135",
            database="/Root/bench",
            static_pids=(10,),
            dynamic_pids=(20,),
        )
        cluster.init_workload.side_effect = lambda command, timeout: (
            self._local_ydb_command_result(command),
            [self._local_ydb_command_result(command)],
        )
        cluster._run.side_effect = lambda command, **_kwargs: self._local_ydb_command_result(command)
        monitor = mock.Mock(records=[])
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
        monitor.summary.return_value = {
            "static_cpu_mean": 11,
            "static_cpu_max": 12,
            "dynamic_cpu_mean": 13,
            "dynamic_cpu_max": 14,
            "cli_cpu_mean": 15,
            "cli_cpu_max": 16,
            "host_cpu_mean": 17,
            "host_cpu_max": 18,
        }
        progress = []
        profile_directory = self.root / "synthetic-profile"
        with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {definition.name: definition}), mock.patch.object(
            local_ydb,
            "LinuxCpuMonitor",
            return_value=monitor,
        ), mock.patch.object(
            local_ydb,
            "atomic_write_text",
        ), mock.patch.object(
            local_ydb,
            "atomic_write_json",
        ), mock.patch.object(
            local_ydb,
            "run_command",
            side_effect=lambda command, *_args, **_kwargs: self._local_ydb_command_result(command, metrics_output),
        ) as execute:
            lifecycle = self._synthetic_workload_lifecycle(
                workload,
                cluster,
                progress,
                command_timeout_seconds=3,
            )
            lifecycle.open_profile(profile_directory, "tpcc-dataset")
            first_metrics, first_commands = lifecycle.run_sample(
                8,
                1,
                1,
                2,
                self.root / "synthetic-repeat-1",
                "ignored-1",
                {"attempt": 1},
            )
            second_metrics, second_commands = lifecycle.run_sample(
                16,
                1,
                2,
                2,
                self.root / "synthetic-repeat-2",
                "ignored-2",
                {"attempt": 2},
            )
            lifecycle.close_profile()
            lifecycle.close_profile()

        self.assertEqual(cluster.init_workload.call_count, 2)
        self.assertEqual([call.args[0][2] for call in cluster.init_workload.call_args_list], ["init", "import"])
        self.assertTrue(all(call.kwargs["timeout"] == 3 for call in cluster.init_workload.call_args_list))
        self.assertEqual(execute.call_count, 2)
        self.assertEqual(cluster._run.call_count, 1)
        self.assertTrue(cluster._run.call_args.kwargs["ignore_cancellation"])
        self.assertEqual(cluster._run.call_args.kwargs["timeout"], 3)
        self.assertEqual(
            [item["phase"] for item in progress],
            [
                "initializing-workload",
                "preparing-import",
                "measuring",
                "measuring",
                "cleaning-workload",
            ],
        )
        self.assertNotIn("warming-up", [item["phase"] for item in progress])
        self.assertEqual([len(first_commands), len(second_commands)], [1, 1])
        self.assertEqual(first_metrics["static_cpu_mean"], 11)
        self.assertEqual(second_metrics["load"], 16)
        self.assertEqual(
            monitor.summary.call_args_list,
            [
                mock.call(started_at_unix=101.0, finished_at_unix=109.0),
                mock.call(started_at_unix=101.0, finished_at_unix=109.0),
            ],
        )
        for call in execute.call_args_list:
            argv = call.args[0]
            self.assertEqual(argv[argv.index("--warmup") + 1], 5)
            self.assertEqual(call.args[2], 3)
        profile_commands = lifecycle.profile_commands
        self.assertEqual([item["argv"][2] for item in profile_commands], ["init", "import", "clean"])

    def test_local_ydb_geometry_lifecycle_shares_dataset_and_requires_matching_nodes(self):
        definition, workload = self._synthetic_profile_workload(dataset_scope="geometry")
        metrics_output = """
            Total Txs Txs/Sec Retries Errors p50(ms) p95(ms) p99(ms) pMax(ms)
            10 10 0 0 1 2 3 4
        """
        cluster = mock.Mock(
            ydb_cli=Path("/tmp/ydb"),
            client_endpoint="grpc://benchmark-host:2135",
            database="/Root/bench",
            static_pids=(10,),
            dynamic_pids=(20,),
        )
        cluster.init_workload.side_effect = lambda command, timeout: (
            self._local_ydb_command_result(command),
            [self._local_ydb_command_result(command)],
        )
        cluster._run.side_effect = lambda command, **_kwargs: self._local_ydb_command_result(command)
        monitor = mock.Mock(records=[])
        monitor.stop.return_value = {}
        monitor.summary.return_value = {}
        progress = []
        with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {definition.name: definition}), mock.patch.object(
            local_ydb,
            "LinuxCpuMonitor",
            return_value=monitor,
        ), mock.patch.object(local_ydb, "atomic_write_text"), mock.patch.object(
            local_ydb,
            "atomic_write_json",
        ), mock.patch.object(
            local_ydb,
            "run_command",
            side_effect=lambda command, *_args, **_kwargs: self._local_ydb_command_result(command, metrics_output),
        ) as execute:
            lifecycle = self._synthetic_workload_lifecycle(workload, cluster, progress)
            lifecycle.open_profile(self.root / "geometry-profile", "ignored-profile")
            lifecycle.open_geometry(
                self.root / "geometry-1",
                "geometry-dataset-1",
                1,
                progress_fields={"search_stage": 3},
            )
            lifecycle.run_sample(8, 1, 1, 2, self.root / "geometry-repeat-1", "ignored-1", {})
            lifecycle.run_sample(16, 1, 2, 2, self.root / "geometry-repeat-2", "ignored-2", {})
            with self.assertRaisesRegex(BenchmarkError, "belongs to 1 dynamic nodes, got 2"):
                lifecycle.run_sample(8, 2, 1, 1, self.root / "geometry-mismatch", "ignored", {})
            lifecycle.close_geometry()
            with self.assertRaisesRegex(BenchmarkError, "dataset is unavailable"):
                lifecycle.run_sample(8, 1, 1, 1, self.root / "geometry-closed", "ignored", {})
            lifecycle.close_profile()

        self.assertEqual(cluster.init_workload.call_count, 2)
        self.assertEqual(execute.call_count, 2)
        self.assertEqual(cluster._run.call_count, 1)
        self.assertTrue(cluster._run.call_args.kwargs["ignore_cancellation"])
        self.assertEqual(progress[0]["search_stage"], 3)
        self.assertEqual([item["argv"][2] for item in lifecycle.geometry_commands], ["init", "import", "clean"])

    def test_local_ydb_geometry_dataset_is_cleaned_before_dynamic_node_scaling(self):
        configuration = load_config(self._config("""
            local-ydb:
              geometry-scaling:
                workload: {type: kv, operation: upsert}
                geometry: {preset: storage, static-nodes: 1, dynamic-nodes: 1, max-dynamic-nodes: 2}
                load: {parameter: threads, values: [1]}
                measurement: {warmup: 0, duration: 1, repetitions: 1}
                affinity:
                  ydb-cli: {mode: none}
                  static-nodes: {mode: none}
                  dynamic-nodes: {mode: none}
        """)).runs[0]
        definition = replace(local_ydb_workloads.workload_definition("kv"), dataset_scope="geometry")
        actions = []
        with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {"kv": definition}):
            manifest, _output, _events = self._run_mock_local_ydb(
                configuration,
                "geometry-scaling",
                [{"throughput": 10}, {"throughput": 20}],
                cpu_metrics={"dynamic_cpu_mean": 100, "static_cpu_mean": 1},
                lifecycle_actions=actions,
            )

        self.assertEqual(manifest["result"]["dynamic_nodes"], 2)
        self.assertEqual(
            actions,
            [
                ("init", "ydb_bench_geometry_01"),
                ("measure", "ydb_bench_geometry_01"),
                ("clean", "ydb_bench_geometry_01"),
                ("scale", None),
                ("init", "ydb_bench_geometry_02"),
                ("measure", "ydb_bench_geometry_02"),
                ("clean", "ydb_bench_geometry_02"),
            ],
        )

    def test_local_ydb_geometry_cleanup_cancellation_prevents_scaling(self):
        configuration = load_config(self._config("""
            local-ydb:
              geometry-cancel-scaling:
                workload: {type: kv, operation: upsert}
                geometry: {preset: storage, static-nodes: 1, dynamic-nodes: 1, max-dynamic-nodes: 2}
                load: {parameter: threads, values: [1]}
                measurement: {warmup: 0, duration: 1, repetitions: 1}
                affinity:
                  ydb-cli: {mode: none}
                  static-nodes: {mode: none}
                  dynamic-nodes: {mode: none}
        """)).runs[0]
        definition = replace(local_ydb_workloads.workload_definition("kv"), dataset_scope="geometry")
        cancel_event = threading.Event()
        actions = []
        with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {"kv": definition}):
            with self.assertRaisesRegex(BenchmarkInterrupted, "was cancelled"):
                self._run_mock_local_ydb(
                    configuration,
                    "geometry-cancel-scaling",
                    [{"throughput": 10}],
                    cancel_event=cancel_event,
                    cleanup_action=cancel_event.set,
                    cpu_metrics={"dynamic_cpu_mean": 100, "static_cpu_mean": 1},
                    lifecycle_actions=actions,
                )

        self.assertEqual(
            actions,
            [
                ("init", "ydb_bench_geometry_01"),
                ("measure", "ydb_bench_geometry_01"),
                ("clean", "ydb_bench_geometry_01"),
            ],
        )
        self.last_local_ydb_cluster.add_dynamic_nodes.assert_not_called()
        cleanup = self.last_local_ydb_cluster._run.call_args
        self.assertTrue(cleanup.kwargs["ignore_cancellation"])
        manifest = json.loads((self.root / "geometry-cancel-scaling" / "run.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["status"], "interrupted")
        self.assertEqual(manifest["state"], "cancelled")

    def test_local_ydb_final_geometry_dataset_is_reused_for_verification(self):
        configuration = load_config(self._config("""
            local-ydb:
              geometry-verification:
                workload: {type: kv, operation: upsert}
                load: {parameter: threads, values: [1]}
                measurement: {warmup: 0, duration: 1, repetitions: 1, verification-repetitions: 2}
                affinity:
                  ydb-cli: {mode: none}
                  static-nodes: {mode: none}
                  dynamic-nodes: {mode: none}
        """)).runs[0]
        definition = replace(local_ydb_workloads.workload_definition("kv"), dataset_scope="geometry")
        actions = []
        with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {"kv": definition}):
            manifest, _output, _events = self._run_mock_local_ydb(
                configuration,
                "geometry-verification",
                [{"throughput": 10}, {"throughput": 11}, {"throughput": 12}],
                lifecycle_actions=actions,
            )

        self.assertEqual(manifest["verification"]["status"], "completed")
        self.assertEqual(
            actions,
            [
                ("init", "ydb_bench_geometry_01"),
                ("measure", "ydb_bench_geometry_01"),
                ("measure", "ydb_bench_geometry_01"),
                ("measure", "ydb_bench_geometry_01"),
                ("clean", "ydb_bench_geometry_01"),
            ],
        )

    def test_local_ydb_geometry_cleanup_does_not_mask_primary_error(self):
        configuration = load_config(self._config("""
            local-ydb:
              geometry-failure:
                workload: {type: kv, operation: upsert}
                load: {parameter: threads, values: [1]}
                measurement: {warmup: 0, duration: 1, repetitions: 1}
                affinity:
                  ydb-cli: {mode: none}
                  static-nodes: {mode: none}
                  dynamic-nodes: {mode: none}
        """)).runs[0]
        definition = replace(local_ydb_workloads.workload_definition("kv"), dataset_scope="geometry")

        def fail_cleanup():
            raise BenchmarkError("geometry cleanup failed")

        with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {"kv": definition}):
            with self.assertRaisesRegex(OSError, "measurement failed"):
                self._run_mock_local_ydb(
                    configuration,
                    "geometry-failure",
                    [OSError("measurement failed")],
                    cleanup_action=fail_cleanup,
                )

        self.assertEqual(self.last_local_ydb_cluster._run.call_count, 1)
        self.assertTrue(self.last_local_ydb_cluster._run.call_args.kwargs["ignore_cancellation"])

    def test_local_ydb_profile_lifecycle_cleans_partial_prepare_without_masking_error(self):
        definition, workload = self._synthetic_profile_workload()
        cluster = mock.Mock(
            ydb_cli=Path("/tmp/ydb"),
            client_endpoint="grpc://benchmark-host:2135",
            database="/Root/bench",
        )
        initialized = self._local_ydb_command_result(("ydb", "synthetic", "init"))
        cluster.init_workload.side_effect = ((initialized, [initialized]), BenchmarkError("import failed"))
        cluster._run.side_effect = lambda command, **_kwargs: self._local_ydb_command_result(command)
        progress = []
        with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {definition.name: definition}), mock.patch.object(
            local_ydb,
            "atomic_write_text",
        ), mock.patch.object(local_ydb, "atomic_write_json"):
            lifecycle = self._synthetic_workload_lifecycle(
                workload,
                cluster,
                progress,
            )
            with self.assertRaisesRegex(BenchmarkError, "import failed"):
                lifecycle.open_profile(self.root / "partial-profile", "tpcc-dataset")
            lifecycle.close_profile()

        self.assertEqual(cluster.init_workload.call_count, 2)
        self.assertEqual(cluster._run.call_count, 1)
        self.assertTrue(cluster._run.call_args.kwargs["ignore_cancellation"])

    def test_local_ydb_inline_lifecycle_rejects_huge_cpu_measurement_window(self):
        definition, workload = self._synthetic_profile_workload(measurement_window=(10**1000, 10**1000 + 1))
        cluster = mock.Mock(
            ydb_cli=Path("/tmp/ydb"),
            client_endpoint="grpc://benchmark-host:2135",
            database="/Root/bench",
            static_pids=(10,),
            dynamic_pids=(20,),
        )
        cluster.init_workload.side_effect = lambda command, timeout: (
            self._local_ydb_command_result(command),
            [self._local_ydb_command_result(command)],
        )
        cluster._run.side_effect = lambda command, **_kwargs: self._local_ydb_command_result(command)
        monitor = mock.Mock(records=[])
        monitor.stop.return_value = {}
        progress = []
        with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {definition.name: definition}), mock.patch.object(
            local_ydb,
            "LinuxCpuMonitor",
            return_value=monitor,
        ), mock.patch.object(local_ydb, "atomic_write_text"), mock.patch.object(
            local_ydb,
            "atomic_write_json",
        ), mock.patch.object(
            local_ydb,
            "run_command",
            side_effect=lambda command, *_args, **_kwargs: self._local_ydb_command_result(command),
        ):
            lifecycle = self._synthetic_workload_lifecycle(workload, cluster, progress)
            lifecycle.open_profile(self.root / "huge-window-profile", "tpcc-dataset")
            with self.assertRaisesRegex(BenchmarkError, "invalid CPU measurement window") as caught:
                lifecycle.run_sample(
                    8,
                    1,
                    1,
                    1,
                    self.root / "huge-window-repeat",
                    "ignored",
                    {},
                )
            lifecycle.close_profile(primary_error=caught.exception)

        monitor.summary.assert_not_called()
        self.assertEqual(cluster._run.call_count, 1)

    def test_local_ydb_profile_lifecycle_preserves_run_error_when_cleanup_fails(self):
        definition, workload = self._synthetic_profile_workload()
        cluster = mock.Mock(
            ydb_cli=Path("/tmp/ydb"),
            client_endpoint="grpc://benchmark-host:2135",
            database="/Root/bench",
            static_pids=(10,),
            dynamic_pids=(20,),
        )
        cluster.init_workload.side_effect = lambda command, timeout: (
            self._local_ydb_command_result(command),
            [self._local_ydb_command_result(command)],
        )
        cluster._run.side_effect = BenchmarkError("cleanup failed")
        monitor = mock.Mock(records=[])
        monitor.stop.return_value = {}
        progress = []
        failed_run = self._local_ydb_command_result(("ydb", "synthetic", "run"), exit_code=17)
        with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {definition.name: definition}), mock.patch.object(
            local_ydb,
            "LinuxCpuMonitor",
            return_value=monitor,
        ), mock.patch.object(local_ydb, "atomic_write_text") as write_text, mock.patch.object(
            local_ydb,
            "atomic_write_json",
        ), mock.patch.object(
            local_ydb, "run_command", return_value=failed_run
        ):
            lifecycle = self._synthetic_workload_lifecycle(
                workload,
                cluster,
                progress,
            )
            lifecycle.open_profile(self.root / "run-error-profile", "tpcc-dataset")
            with self.assertRaisesRegex(BenchmarkError, "exited with code 17") as caught:
                lifecycle.run_sample(
                    8,
                    1,
                    1,
                    1,
                    self.root / "run-error-repeat",
                    "ignored",
                    {},
                )
            lifecycle.close_profile(primary_error=caught.exception)

        self.assertEqual(cluster._run.call_count, 1)
        self.assertTrue(cluster._run.call_args.kwargs["ignore_cancellation"])
        self.assertIn("clean.error.txt", [Path(call.args[0]).name for call in write_text.call_args_list])

    def test_local_ydb_profile_lifecycle_attempts_all_cleanup_steps_and_only_cleanup_error_fails(self):
        definition, workload = self._synthetic_profile_workload(("clean", "vacuum"))
        cluster = mock.Mock(
            ydb_cli=Path("/tmp/ydb"),
            client_endpoint="grpc://benchmark-host:2135",
            database="/Root/bench",
        )
        cluster.init_workload.side_effect = lambda command, timeout: (
            self._local_ydb_command_result(command),
            [self._local_ydb_command_result(command)],
        )
        cluster._run.side_effect = (
            OSError("clean failed"),
            self._local_ydb_command_result(("ydb", "synthetic", "vacuum")),
        )
        progress = []
        with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {definition.name: definition}), mock.patch.object(
            local_ydb,
            "atomic_write_text",
        ), mock.patch.object(local_ydb, "atomic_write_json"):
            lifecycle = self._synthetic_workload_lifecycle(
                workload,
                cluster,
                progress,
            )
            lifecycle.open_profile(self.root / "cleanup-error-profile", "tpcc-dataset")
            with self.assertRaisesRegex(BenchmarkError, "clean failed"):
                lifecycle.close_profile()
            lifecycle.close_profile()

        self.assertEqual(cluster._run.call_count, 2)
        self.assertTrue(all(call.kwargs["ignore_cancellation"] for call in cluster._run.call_args_list))

    def test_local_ydb_profile_lifecycle_runs_cleanup_when_progress_fails(self):
        class FailingCleanupProgress(list):
            def append(self, item):
                if item["phase"].startswith("cleaning-"):
                    raise OSError("cleanup progress failed")
                super().append(item)

        definition, workload = self._synthetic_profile_workload(("clean", "vacuum"))
        cluster = mock.Mock(
            ydb_cli=Path("/tmp/ydb"),
            client_endpoint="grpc://benchmark-host:2135",
            database="/Root/bench",
        )
        cluster.init_workload.side_effect = lambda command, timeout: (
            self._local_ydb_command_result(command),
            [self._local_ydb_command_result(command)],
        )
        cluster._run.side_effect = lambda command, **_kwargs: self._local_ydb_command_result(command)
        progress = FailingCleanupProgress()
        with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {definition.name: definition}), mock.patch.object(
            local_ydb,
            "atomic_write_text",
        ), mock.patch.object(local_ydb, "atomic_write_json"):
            lifecycle = self._synthetic_workload_lifecycle(workload, cluster, progress)
            lifecycle.open_profile(self.root / "cleanup-progress-error-profile", "tpcc-dataset")
            with self.assertRaisesRegex(BenchmarkError, "cleanup progress failed"):
                lifecycle.close_profile()

        self.assertEqual(cluster._run.call_count, 2)
        self.assertTrue(all(call.kwargs["ignore_cancellation"] for call in cluster._run.call_args_list))

    def test_local_ydb_profile_lifecycle_cancellation_cleanup_ignores_cancel_event(self):
        definition, workload = self._synthetic_profile_workload()
        cluster = mock.Mock(
            ydb_cli=Path("/tmp/ydb"),
            client_endpoint="grpc://benchmark-host:2135",
            database="/Root/bench",
            static_pids=(10,),
            dynamic_pids=(20,),
        )
        cluster.init_workload.side_effect = lambda command, timeout: (
            self._local_ydb_command_result(command),
            [self._local_ydb_command_result(command)],
        )
        cluster._run.side_effect = lambda command, **_kwargs: self._local_ydb_command_result(command)
        monitor = mock.Mock(records=[])
        monitor.stop.return_value = {}
        progress = []
        interrupted = self._local_ydb_command_result(("ydb", "synthetic", "run"), interrupted=True)
        cancel_event = threading.Event()
        with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {definition.name: definition}), mock.patch.object(
            local_ydb,
            "LinuxCpuMonitor",
            return_value=monitor,
        ), mock.patch.object(local_ydb, "atomic_write_text"), mock.patch.object(
            local_ydb,
            "atomic_write_json",
        ), mock.patch.object(
            local_ydb, "run_command", return_value=interrupted
        ):
            lifecycle = self._synthetic_workload_lifecycle(
                workload,
                cluster,
                progress,
                cancel_event,
            )
            lifecycle.open_profile(self.root / "cancel-profile", "tpcc-dataset")
            with self.assertRaisesRegex(BenchmarkInterrupted, "was interrupted") as caught:
                lifecycle.run_sample(
                    8,
                    1,
                    1,
                    1,
                    self.root / "cancel-repeat",
                    "ignored",
                    {},
                )
            cancel_event.set()
            lifecycle.close_profile(primary_error=caught.exception)

        self.assertEqual(cluster._run.call_count, 1)
        self.assertTrue(cluster._run.call_args.kwargs["ignore_cancellation"])

    def test_local_ydb_profile_lifecycle_cleanup_artifact_failures_do_not_mask_primary(self):
        definition, workload = self._synthetic_profile_workload(("clean", "vacuum"))
        cluster = mock.Mock(
            ydb_cli=Path("/tmp/ydb"),
            client_endpoint="grpc://benchmark-host:2135",
            database="/Root/bench",
        )
        cluster.init_workload.side_effect = lambda command, timeout: (
            self._local_ydb_command_result(command),
            [self._local_ydb_command_result(command)],
        )
        cluster._run.side_effect = lambda command, **_kwargs: self._local_ydb_command_result(command)
        progress = []
        with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {definition.name: definition}), mock.patch.object(
            local_ydb,
            "atomic_write_text",
        ) as write_text, mock.patch.object(local_ydb, "atomic_write_json") as write_json:
            lifecycle = self._synthetic_workload_lifecycle(
                workload,
                cluster,
                progress,
            )
            lifecycle.open_profile(self.root / "artifact-error-profile", "tpcc-dataset")
            primary = BenchmarkInterrupted("cancelled")
            write_text.side_effect = OSError("text write failed")
            write_json.side_effect = OSError("json write failed")
            lifecycle.close_profile(primary_error=primary)

        self.assertEqual(cluster._run.call_count, 2)
        self.assertTrue(all(call.kwargs["ignore_cancellation"] for call in cluster._run.call_args_list))

    def test_local_ydb_unexpected_profile_error_cleans_before_terminal_failure(self):
        configuration = load_config(self._config("""
            local-ydb:
              unexpected-failure:
                workload: {type: kv, operation: upsert}
                load: {parameter: rate, values: [10]}
                measurement: {warmup: 0, duration: 1, repetitions: 1}
                affinity:
                  ydb-cli: {mode: none}
                  static-nodes: {mode: none}
                  dynamic-nodes: {mode: none}
        """)).runs[0]
        definition = replace(local_ydb_workloads.workload_definition("kv"), dataset_scope="profile")

        with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {"kv": definition}):
            with self.assertRaisesRegex(OSError, "measurement failed"):
                self._run_mock_local_ydb(
                    configuration,
                    "unexpected-profile-error",
                    [OSError("measurement failed")],
                )

        manifest = json.loads((self.root / "unexpected-profile-error" / "run.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["status"], "failed")
        self.assertEqual(manifest["state"], "failed")
        self.assertEqual(manifest["progress"]["phase"], "failed")
        self.assertEqual(self.last_local_ydb_cluster._run.call_count, 1)
        cleanup = self.last_local_ydb_cluster._run.call_args
        self.assertIn("clean", cleanup.args[0])
        self.assertTrue(cleanup.kwargs["ignore_cancellation"])
        self.last_local_ydb_cluster.stop.assert_called_once_with()

    def test_local_ydb_cancelled_during_cleanup_never_publishes_success(self):
        configuration = load_config(self._config("""
            local-ydb:
              cleanup-cancellation:
                workload: {type: kv, operation: upsert}
                load: {parameter: rate, values: [10]}
                measurement: {warmup: 0, duration: 1, repetitions: 1}
                affinity:
                  ydb-cli: {mode: none}
                  static-nodes: {mode: none}
                  dynamic-nodes: {mode: none}
        """)).runs[0]
        cancel_event = threading.Event()

        with self.assertRaisesRegex(BenchmarkInterrupted, "was cancelled"):
            self._run_mock_local_ydb(
                configuration,
                "cleanup-cancellation",
                [{"throughput": 10}],
                cancel_event=cancel_event,
                cleanup_action=cancel_event.set,
            )

        manifest = json.loads((self.root / "cleanup-cancellation" / "run.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["status"], "interrupted")
        self.assertEqual(manifest["state"], "cancelled")
        self.assertEqual(manifest["progress"]["phase"], "cancelled")
        self.assertEqual(self.last_local_ydb_cluster._run.call_count, 1)
        self.assertTrue(self.last_local_ydb_cluster._run.call_args.kwargs["ignore_cancellation"])
        self.last_local_ydb_cluster.stop.assert_called_once_with()

    def test_local_ydb_cleanup_plan_uses_bounded_default_timeout(self):
        cluster = local_ydb.LocalYdbCluster(
            self.root / "ydbd",
            self.root / "ydb",
            self.root / "process_guard",
            self.root / "cluster-timeout",
            {"static_nodes": 1, "dynamic_nodes": 1},
            {"ydb_cli": None, "static_nodes": None, "dynamic_nodes": None},
            10000,
        )
        cli_context = local_ydb_workloads.WorkloadCli(cluster.ydb_cli, "grpc://host:2135", cluster.database)
        plan = local_ydb_workloads.build_cleanup_plan(
            cli_context,
            "table-prefix",
            {"type": "kv"},
        )[0]
        result = self._local_ydb_command_result(plan.argv)
        with mock.patch.object(local_ydb, "run_command", return_value=result) as execute:
            cluster._run(plan.argv, timeout=plan.timeout_seconds, ignore_cancellation=True)

        self.assertEqual(plan.timeout_seconds, 120)
        self.assertEqual(execute.call_args.args[2], 120)

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

    def test_local_ydb_tpcc_json_result_uses_uncapped_throughput_and_admitted_latency(self):
        workload = local_ydb_workloads.normalize_workload(
            {
                "type": "tpcc",
                "operation": "run",
                "options": {"warehouses": 2, "latency-transaction": "Payment"},
            },
            "workload",
        )
        request = local_ydb_workloads.WorkloadRunRequest("max-sessions", 20, 10, 2, 2, None)
        payload = self._tpcc_result_payload()
        command_result = self._local_ydb_command_result(
            ("ydb", "workload", "tpcc", "run"),
            json.dumps(payload),
        )
        result = local_ydb_workloads.parse_workload_result("tpcc", command_result, workload, request)
        self.assertEqual(
            result.metrics,
            {
                "transactions": 300,
                "new_orders": 120,
                "throughput": 12,
                "cli_elapsed_seconds": 12,
                "tpcc_tpmc": 25.5,
                "efficiency_pct": 80.25,
                "errors": 10,
                "p50_ms": 11,
                "p90_ms": 12,
                "p95_ms": 13,
                "p99_ms": 14,
                "p999_ms": 15,
            },
        )
        self.assertEqual(result.details, payload)
        self.assertEqual(result.measurement_window, (1001.0, 1010.0))
        schema = local_ydb_workloads.workload_result_schema("tpcc")
        self.assertEqual(schema["schema_id"], "tpcc-json-v3")
        self.assertEqual(schema["throughput_unit"], "new orders/s")
        self.assertEqual(schema["slo_metrics"]["p999"], "p999_ms")
        self.assertNotIn("p99.9", schema["slo_metrics"])

        zero_payload = self._tpcc_result_payload(new_orders=0, selected_ok=0)
        for name in ("NewOrder", "Payment"):
            transaction = zero_payload["transactions"][name]
            for field in ("percentiles", "percentiles_ms", "percentiles_pure"):
                transaction[field] = {key: 0 for key in transaction[field]}
        zero_result = local_ydb_workloads.parse_workload_result(
            "tpcc",
            self._local_ydb_command_result(("ydb",), json.dumps(zero_payload)),
            workload,
            request,
        )
        self.assertEqual(zero_result.metrics["transactions"], 0)
        self.assertEqual(zero_result.metrics["new_orders"], 0)
        self.assertEqual(zero_result.metrics["throughput"], 0)
        self.assertEqual(zero_result.metrics["p999_ms"], 0)

        no_new_orders = self._tpcc_result_payload(new_orders=0, selected_ok=300)
        for field in ("percentiles", "percentiles_ms", "percentiles_pure"):
            no_new_orders["transactions"]["NewOrder"][field] = {
                key: 0 for key in no_new_orders["transactions"]["NewOrder"][field]
            }
        no_new_orders_result = local_ydb_workloads.parse_workload_result(
            "tpcc",
            self._local_ydb_command_result(("ydb",), json.dumps(no_new_orders)),
            workload,
            request,
        )
        self.assertEqual(no_new_orders_result.metrics["transactions"], 0)
        self.assertEqual(no_new_orders_result.metrics["p99_ms"], 14)
        aggregated = local_ydb._aggregate_measurements(
            [no_new_orders_result.metrics],
            local_ydb_workloads.workload_definition("tpcc").result_adapter.metrics,
        )
        passed, reason = load_control.evaluate_load(
            {
                "parameter": "max-sessions",
                "allow_errors": True,
                "search": {"start": 1, "maximum": 20},
                "objective": {"type": "latency-slo"},
            },
            20,
            aggregated,
        )
        self.assertFalse(passed)
        self.assertIn("zero successful operations", reason)

    def test_local_ydb_tpcc_json_result_rejects_malformed_or_inconsistent_output(self):
        workload = local_ydb_workloads.normalize_workload(
            {
                "type": "tpcc",
                "operation": "run",
                "options": {"warehouses": 2, "latency-transaction": "Payment"},
            },
            "workload",
        )
        request = local_ydb_workloads.WorkloadRunRequest("max-sessions", 20, 10, 2, 2, None)

        def changed(change):
            value = self._tpcc_result_payload()
            change(value)
            return json.dumps(value)

        invalid = (
            ("not json", "malformed"),
            (" " * (1024 * 1024 + 1), "exceeds 1048576 bytes"),
            (json.dumps({"error": "Stopped before measurements"}), "fatal error.*Stopped before measurements"),
            (json.dumps({"summary": {}}), r"field \$.*missing.*transactions"),
            (
                changed(lambda value: value["summary"].__setitem__("unexpected", 1)),
                "summary.*unknown fields: unexpected",
            ),
            (
                changed(lambda value: value["summary"].__setitem__("threads", True)),
                "summary.threads.*integer",
            ),
            (
                changed(lambda value: value["summary"].__setitem__("tpmc", float("nan"))),
                "summary.tpmc.*finite number",
            ),
            (
                changed(lambda value: value["summary"].__setitem__("efficiency", -1)),
                "summary.efficiency.*finite number",
            ),
            (
                changed(lambda value: value["summary"].__setitem__("warehouses", 3)),
                "summary.warehouses.*configured warehouses",
            ),
            (
                changed(lambda value: value["summary"].__setitem__("max_sessions", 19)),
                "summary.max_sessions.*requested load",
            ),
            (
                changed(lambda value: value["summary"].__setitem__("threads", 1)),
                "summary.threads.*requested client threads",
            ),
            (
                changed(lambda value: value["summary"].__setitem__("warmup_seconds", 3)),
                "summary.warmup_seconds.*effective warmup",
            ),
            (
                changed(lambda value: value["summary"].__setitem__("time_seconds", 9)),
                "summary.time_seconds.*measurement window",
            ),
            (
                changed(lambda value: value["summary"].__setitem__("time_seconds", 71)),
                "summary.time_seconds.*measurement window",
            ),
            (
                changed(lambda value: value["summary"].__setitem__("new_orders", 119)),
                "summary.new_orders.*NewOrder.ok_count",
            ),
            (
                changed(lambda value: value["transactions"]["Payment"]["percentiles"].update({"90": 7, "95": 3})),
                "Payment.percentiles.*monotonic",
            ),
            (
                changed(lambda value: value["transactions"]["Payment"]["percentiles"].__setitem__("50", 0)),
                "Payment.*percentiles must be positive",
            ),
            (
                changed(lambda value: value["transactions"]["Payment"]["percentiles"].__setitem__("50", True)),
                "Payment.percentiles.50.*integer",
            ),
            (
                changed(lambda value: value["transactions"].__setitem__("Extra", {})),
                "transactions.*unknown fields: Extra",
            ),
        )
        for stdout, message in invalid:
            with self.subTest(message=message), self.assertRaisesRegex(BenchmarkError, message):
                local_ydb_workloads.parse_workload_result(
                    "tpcc",
                    self._local_ydb_command_result(("ydb",), stdout),
                    workload,
                    request,
                )

    def test_local_ydb_topic_window_result_aggregates_exact_measurement_windows(self):
        workload = local_ydb_workloads.normalize_workload(
            {"type": "topic", "operation": "full", "options": {"consumers": 2}},
            "workload",
        )
        request = local_ydb_workloads.WorkloadRunRequest("rate", 150, 10, 3, 4, None)

        def row(index, metrics, second):
            return "{} {} 2026-08-25T10:00:{:02d}Z".format(index, " ".join(map(str, metrics)), second)

        rows = [row(3, [0] * 9, 3)]
        for offset, index in enumerate(range(4, 14)):
            rows.append(
                row(
                    index,
                    [
                        100 + offset,
                        10 + offset,
                        1 + offset,
                        20 + offset,
                        30 + offset,
                        40 + offset,
                        200 + 2 * offset,
                        50 + offset,
                        60 + offset,
                    ],
                    index,
                )
            )
        rows.append(row("Total", [999] * 9, 14))
        stdout = "\n".join(rows)
        result = local_ydb_workloads.parse_workload_result(
            "topic",
            self._local_ydb_command_result(("ydb", "workload", "topic", "run", "full"), stdout),
            workload,
            request,
        )
        self.assertEqual(
            result.metrics,
            {
                "transactions": 1045,
                "throughput": 104.5,
                "write_messages_s": 104.5,
                "write_mib_s": 14.5,
                "write_p99_ms": 10,
                "inflight_p99_messages": 29,
                "lag_p99_messages": 39,
                "lag_p99_ms": 49,
                "read_messages_s": 209,
                "read_per_consumer_messages_s": 104.5,
                "read_mib_s": 54.5,
                "full_p99_ms": 69,
            },
        )
        self.assertEqual(
            result.details,
            {
                "percentile": 99,
                "window_seconds": 1,
                "measurement_windows": 10,
                "rate_aggregation": "mean",
                "percentile_aggregation": "maximum",
            },
        )
        self.assertEqual(result.measurement_window, (1787652003.0, 1787652013.0))
        passed, reason = load_control.evaluate_load(
            {
                "parameter": "rate",
                "allow_errors": False,
                "search": {"start": 10, "maximum": 150},
                "objective": {
                    "type": "latency-slo",
                    "percentile": "p99",
                    "latency_metric": "full_p99_ms",
                    "max_ms": 100,
                    "max_errors": 0,
                    "min_achieved_rate_ratio": 0.98,
                },
            },
            150,
            result.metrics,
        )
        self.assertFalse(passed)
        self.assertIn("achieved rate ratio 0.6967", reason)

        schema = local_ydb_workloads.workload_result_schema("topic")
        self.assertEqual(schema["schema_id"], "topic-window-v1")
        self.assertEqual(schema["throughput_unit"], "messages/s")
        self.assertEqual(schema["slo_metrics"], {"p99": "full_p99_ms"})
        self.assertFalse(schema["reports_errors"])
        self.assertEqual(
            {metric["name"]: metric["unit"] for metric in schema["metrics"]}["read_messages_s"],
            "deliveries/s",
        )

        zero_stdout = "\n".join([row(index, [0] * 9, index) for index in range(3, 14)] + [row("Total", [999] * 9, 14)])
        zero_result = local_ydb_workloads.parse_workload_result(
            "topic",
            self._local_ydb_command_result(("ydb",), zero_stdout),
            workload,
            request,
        )
        self.assertEqual(zero_result.metrics["transactions"], 0)
        self.assertEqual(zero_result.metrics["throughput"], 0)
        aggregated = local_ydb._aggregate_measurements(
            [zero_result.metrics],
            local_ydb_workloads.workload_definition("topic").result_adapter.metrics,
        )
        passed, reason = load_control.evaluate_load(
            {"parameter": "rate", "allow_errors": False, "values": [150]},
            150,
            aggregated,
        )
        self.assertFalse(passed)
        self.assertIn("zero successful operations", reason)

    def test_local_ydb_topic_window_result_supports_zero_warmup(self):
        workload = local_ydb_workloads.normalize_workload(
            {"type": "topic", "operation": "full"},
            "workload",
        )
        request = local_ydb_workloads.WorkloadRunRequest("rate", 100, 2, 0, 1, None)
        stdout = "\n".join(
            (
                "1 10 20 3 4 5 6 30 40 7 2026-08-25T10:00:01Z",
                "2 20 30 8 9 10 11 40 50 12 2026-08-25T10:00:02Z",
                "Total 999 999 999 999 999 999 999 999 999 2026-08-25T10:00:03Z",
            )
        )
        result = local_ydb_workloads.parse_workload_result(
            "topic",
            self._local_ydb_command_result(("ydb",), stdout),
            workload,
            request,
        )
        self.assertEqual(result.metrics["write_messages_s"], 15)
        self.assertEqual(result.metrics["write_p99_ms"], 8)
        self.assertEqual(result.metrics["read_messages_s"], 35)
        self.assertEqual(result.metrics["full_p99_ms"], 12)
        self.assertEqual(result.measurement_window, (1787652000.0, 1787652002.0))

    def test_local_ydb_topic_window_result_rejects_ambiguous_or_unsafe_output(self):
        workload = local_ydb_workloads.normalize_workload(
            {"type": "topic", "operation": "full"},
            "workload",
        )
        request = local_ydb_workloads.WorkloadRunRequest("rate", 100, 2, 1, 1, None)

        def row(index, timestamp, metrics="1 2 3 4 5 6 7 8 9"):
            return "{} {} {}".format(index, metrics, timestamp)

        boundary = row(1, "2026-08-25T10:00:01Z")
        start = row(2, "2026-08-25T10:00:02Z")
        finish = row(3, "2026-08-25T10:00:03Z")
        total = row("Total", "2026-08-25T10:00:04Z")
        valid = "\n".join((boundary, start, finish, total))
        invalid = (
            (None, "is not text"),
            ("\ud800", "not valid UTF-8 text"),
            ("\n".join((boundary, start, finish)), "exactly one Total row"),
            (valid + "\n" + total, "exactly one Total row"),
            ("\n".join((boundary, start, finish, "Total 1 2 3 4 5 6 7 8 9")), "Total row.*11 columns"),
            (valid + " extra", "Total row.*11 columns"),
            (
                "\n".join((boundary, start, finish, row("Total", "2026-08-25T10:00:04Z", "-1 2 3 4 5 6 7 8 9"))),
                "Total row metrics.*unsigned 64-bit",
            ),
            (
                "\n".join(
                    (
                        boundary,
                        start,
                        finish,
                        row("Total", "2026-08-25T10:00:04Z", "1.0 2 3 4 5 6 7 8 9"),
                    )
                ),
                "Total row metrics.*unsigned 64-bit",
            ),
            (
                "\n".join((boundary, start, finish, row("Total", "2026-08-25T10:00:04Z", "١ 2 3 4 5 6 7 8 9"))),
                "Total row metrics.*unsigned 64-bit",
            ),
            (
                "\n".join(
                    (
                        boundary,
                        start,
                        finish,
                        row("Total", "2026-08-25T10:00:04Z", "000000000000000000001 2 3 4 5 6 7 8 9"),
                    )
                ),
                "Total row metrics.*unsigned 64-bit",
            ),
            (
                "\n".join(
                    (
                        boundary,
                        start,
                        finish,
                        row("Total", "2026-08-25T10:00:04Z", "18446744073709551616 2 3 4 5 6 7 8 9"),
                    )
                ),
                "Total row metrics.*unsigned 64-bit",
            ),
            ("\n".join((start, finish, total)), "window row for index 1"),
            ("\n".join((boundary, finish, total)), "window row for index 2"),
            ("\n".join((boundary, start, start, finish, total)), "window row for index 2"),
            (
                "\n".join((boundary, row("٢", "2026-08-25T10:00:02Z"), finish, total)),
                "window row for index 2",
            ),
            (
                "\n".join((boundary, "2 1 2 3 4 5 6 7 8 9", finish, total)),
                "window row 2.*11 columns",
            ),
            (
                "\n".join((boundary, row(2, "2026-08-25T10:00:02Z", "-1 2 3 4 5 6 7 8 9"), finish, total)),
                "window row 2 metrics.*unsigned 64-bit",
            ),
            (
                "\n".join((boundary, start, finish, row("Total", "2026-08-25T10:00:04"))),
                "Total row timestamp.*ISO UTC",
            ),
            (
                "\n".join((boundary, row(2, "2026-08-25T10:00:02+00:00"), finish, total)),
                "window row 2 timestamp.*ISO UTC",
            ),
            (
                "\n".join((boundary, row(2, "2026-02-30T10:00:02Z"), finish, total)),
                "window row 2 timestamp.*ISO UTC",
            ),
            (
                "\n".join((boundary, row(2, "2026-08-25T10:00:03Z"), finish, total)),
                "timestamps must advance by one second",
            ),
            (
                "\n".join((boundary, start, row(3, "2026-08-25T10:00:04Z"), total)),
                "timestamps must advance by one second",
            ),
            ("x" * (1024 * 1024 + 1), "exceeds 1048576 bytes"),
        )
        for stdout, message in invalid:
            with self.subTest(message=message), self.assertRaisesRegex(BenchmarkError, message):
                local_ydb_workloads.parse_workload_result(
                    "topic",
                    self._local_ydb_command_result(("ydb",), stdout),
                    workload,
                    request,
                )

        short_request = replace(request, duration_seconds=1)
        with self.assertRaisesRegex(BenchmarkError, "duration of at least two seconds"):
            local_ydb_workloads.parse_workload_result(
                "topic",
                self._local_ydb_command_result(("ydb",), valid),
                workload,
                short_request,
            )

    def test_local_ydb_result_adapter_rejects_invalid_metric_mappings(self):
        metric_schema = (
            local_ydb_workloads.WorkloadMetric("throughput", "widgets/s", required=True),
            local_ydb_workloads.WorkloadMetric("latency_ms", "ms"),
        )
        request = local_ydb_workloads.WorkloadRunRequest("threads", 4, 10, 0, 8, None)
        command_result = self._local_ydb_command_result(("ydb", "workload", "fake"))
        invalid_results = (
            ({"throughput": 1, "unknown": 2}, "unknown metrics"),
            ({"latency_ms": 2}, "omitted required metrics: throughput"),
            ({"throughput": float("nan")}, "finite non-negative number"),
            ({"throughput": True}, "finite non-negative number"),
            ({"throughput": -1}, "finite non-negative number"),
        )
        for index, (metrics, message) in enumerate(invalid_results):
            with self.subTest(metrics=metrics):
                adapter = local_ydb_workloads.WorkloadResultAdapter(
                    "fake-invalid-{}".format(index),
                    lambda _result, _workload, _request, metrics=metrics: local_ydb_workloads.WorkloadResult(metrics),
                    metric_schema,
                )
                definition = replace(
                    local_ydb_workloads.workload_definition("kv"),
                    result_adapter=adapter,
                    throughput_unit="widgets/s",
                )
                with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {"kv": definition}):
                    with self.assertRaisesRegex(BenchmarkError, message):
                        local_ydb_workloads.parse_workload_result(
                            "kv",
                            command_result,
                            {"type": "kv", "operation": "upsert", "options": {}},
                            request,
                        )

    def test_local_ydb_catalog_rejects_unsafe_result_metric_contracts(self):
        definition = local_ydb_workloads.workload_definition("kv")
        generic = local_ydb_workloads.GENERIC_TOTAL_RESULT

        def changed_metric(name, **changes):
            return tuple(replace(metric, **changes) if metric.name == name else metric for metric in generic.metrics)

        invalid_adapters = (
            (replace(generic, schema_id="x" * 257), "schema id"),
            (replace(generic, metrics=changed_metric("throughput", repetition_aggregation="sum")), "throughput"),
            (replace(generic, metrics=changed_metric("transactions", required=False)), "transactions"),
            (replace(generic, metrics=changed_metric("errors", required=False)), "errors"),
            (replace(generic, metrics=changed_metric("p99_ms", repetition_aggregation="sum")), "SLO metric"),
            (replace(generic, slo_metrics=(("latency", "p99_ms"),)), "SLO metric mapping"),
            (
                local_ydb_workloads.WorkloadResultAdapter(
                    "reserved-v1",
                    generic.parse,
                    (
                        local_ydb_workloads.WorkloadMetric("throughput", "operations/s", required=True),
                        local_ydb_workloads.WorkloadMetric("load", "items"),
                    ),
                ),
                "reserved",
            ),
        )
        for adapter, message in invalid_adapters:
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                local_ydb_workloads._validate_catalog((replace(definition, result_adapter=adapter),))

    def test_local_ydb_result_adapter_repetition_schema_is_consistent_without_fake_errors(self):
        metrics = (
            local_ydb_workloads.WorkloadMetric("throughput", "widgets/s", required=True),
            local_ydb_workloads.WorkloadMetric("latency_ms", "ms"),
        )
        aggregated = local_ydb._aggregate_measurements(
            [{"throughput": 10}, {"throughput": 20}],
            metrics,
        )
        self.assertEqual(aggregated, {"throughput": 15.0})
        self.assertNotIn("errors", aggregated)
        passed, reason = load_control.evaluate_load(
            {"parameter": "threads", "allow_errors": False, "values": [1]},
            1,
            aggregated,
        )
        self.assertTrue(passed)
        self.assertEqual(reason, "configured point")
        with self.assertRaisesRegex(BenchmarkError, "inconsistent metric keys"):
            local_ydb._aggregate_measurements(
                [
                    {"throughput": 10, "latency_ms": 1},
                    {"throughput": 20},
                ],
                metrics,
            )

    def test_local_ydb_custom_result_adapter_writes_details_and_controls_progress_and_cpu_window(self):
        requests = []

        def parse_result(_command_result, normalized_workload, request):
            requests.append((normalized_workload, request))
            return local_ydb_workloads.WorkloadResult(
                {"throughput": 12.5, "latency_ms": 7},
                details={"transactions": {"new-order": 42}},
                measurement_window=(101.0, 109.0),
            )

        adapter = local_ydb_workloads.WorkloadResultAdapter(
            "fake-json-v1",
            parse_result,
            (
                local_ydb_workloads.WorkloadMetric("throughput", "widgets/s", required=True),
                local_ydb_workloads.WorkloadMetric(
                    "latency_ms",
                    "ms",
                    required=True,
                    description="fake SLO latency",
                ),
            ),
            (("p99", "latency_ms"),),
        )

        def build_run_plan(*_args):
            return local_ydb_workloads.WorkloadCommandPlan(
                "run",
                ("ydb", "workload", "fake", "run"),
                30,
                progress_duration_seconds=17,
            )

        definition = replace(
            local_ydb_workloads.workload_definition("kv"),
            warmup_mode="inline",
            run_plan_builder=build_run_plan,
            result_adapter=adapter,
            throughput_unit="widgets/s",
        )
        configuration = load_config(self._config("""
            local-ydb:
              fake-adapter:
                workload: {type: kv, operation: upsert}
                load: {parameter: threads, values: [1]}
                measurement: {warmup: 2, duration: 3, repetitions: 1}
                affinity:
                  ydb-cli: {mode: none}
                  static-nodes: {mode: none}
                  dynamic-nodes: {mode: none}
        """)).runs[0]
        with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {"kv": definition}):
            manifest, output, events = self._run_mock_local_ydb(
                configuration,
                "fake-adapter",
                [{"throughput": 999}],
            )

        self.assertEqual(len(requests), 1)
        request = requests[0][1]
        self.assertEqual(
            (request.load_parameter, request.load, request.duration_seconds, request.warmup_seconds),
            ("threads", 1, 3, 2),
        )
        self.assertEqual(request.client_threads, 64)
        self.assertIsNone(request.objective)
        measuring = [
            event["fields"]["progress"]
            for event in events
            if event["type"] == "step-progress" and event["fields"]["progress"]["phase"] == "measuring"
        ]
        self.assertEqual(measuring[0]["phase_duration_seconds"], 17)
        artifact = json.loads(
            (output / "dynamic-nodes-01" / "load-00000001" / "repeat-001" / "workload-result.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(
            artifact,
            {
                "schema_id": "fake-json-v1",
                "metrics": {"latency_ms": 7, "throughput": 12.5},
                "details": {"transactions": {"new-order": 42}},
            },
        )
        self.assertEqual(manifest["attempts"][0]["throughput"], 12.5)
        self.assertNotIn("errors", manifest["attempts"][0])
        self.assertEqual(manifest["workload_result_schema"]["schema_id"], "fake-json-v1")
        self.assertEqual(manifest["workload_result_schema"]["slo_metrics"], {"p99": "latency_ms"})
        self.assertEqual(manifest["workload_result_schema"]["throughput_unit"], "widgets/s")
        self.assertEqual(manifest["workload_result_schema"]["metrics"][1]["description"], "fake SLO latency")
        repetitions_header = (output / "repetitions.csv").read_text(encoding="utf-8").splitlines()[0]
        summary_header = (output / "summary.csv").read_text(encoding="utf-8").splitlines()[0]
        self.assertIn("latency_ms", repetitions_header)
        self.assertIn("median_latency_ms", summary_header)
        self.assertNotIn("errors", repetitions_header)
        self.assertNotIn("errors", summary_header)
        self.last_local_ydb_monitor.summary.assert_called_once_with(
            started_at_unix=101.0,
            finished_at_unix=109.0,
        )
        self.last_local_ydb_cluster.ensure_running.assert_called()

    def test_local_ydb_result_adapter_maps_configured_slo_to_declared_metric(self):
        adapter = local_ydb_workloads.WorkloadResultAdapter(
            "fake-slo-v1",
            lambda _result, _workload, _request: local_ydb_workloads.WorkloadResult(
                {"throughput": 1, "new_order_p90_ms": 4}
            ),
            (
                local_ydb_workloads.WorkloadMetric("throughput", "widgets/s", required=True),
                local_ydb_workloads.WorkloadMetric("new_order_p90_ms", "ms", required=True),
            ),
            (("p90", "new_order_p90_ms"),),
        )
        definition = replace(
            local_ydb_workloads.workload_definition("kv"),
            result_adapter=adapter,
            throughput_unit="widgets/s",
        )
        with mock.patch.object(local_ydb_workloads, "_WORKLOADS", {"kv": definition}):
            configuration = load_config(self._config("""
                local-ydb:
                  fake-slo:
                    workload: {type: kv, operation: upsert}
                    load:
                      parameter: threads
                      search: {start: 1, maximum: 2}
                      objective: {type: latency-slo, percentile: p90, max-ms: 5}
            """)).runs[0]

        objective = configuration.parameters["local_ydb"]["load"]["objective"]
        self.assertEqual(objective["latency_metric"], "new_order_p90_ms")
        passed, reason = load_control.evaluate_load(
            configuration.parameters["local_ydb"]["load"],
            1,
            {"throughput": 1, "new_order_p90_ms": 4},
        )
        self.assertTrue(passed)
        self.assertIn("does not report request errors", reason)

    def test_local_ydb_cli_total_row_rejects_negative_counters(self):
        for values in ("-1 10 0 0 1 2 3 4", "1 10 -1 0 1 2 3 4", "1 10 0 -1 1 2 3 4"):
            with self.subTest(values=values), self.assertRaisesRegex(BenchmarkError, "valid Total row"):
                parse_cli_metrics("Total Txs Txs/Sec Retries Errors p50(ms) p95(ms) p99(ms) pMax(ms)\n" + values)

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
        self.assertEqual(profile["measurement"]["verification_repetitions"], 0)

    def test_local_ydb_rejects_automatic_search_that_exceeds_attempt_budget(self):
        invalid = self._config("""
            local-ydb:
              unbounded:
                workload: {type: kv, operation: upsert}
                load:
                  parameter: rate
                  search:
                    start: 1
                    maximum: 1000000
                    multiplier: 1.000001
                    resolution-percent: 2
                  objective:
                    type: latency-slo
                    percentile: p99
                    max-ms: 10
        """)
        with self.assertRaisesRegex(
            BenchmarkError,
            r"load\.search.*more than 64 attempts",
        ):
            load_config(invalid)

    def test_local_ydb_verification_config_is_optional_bounded_and_counted_in_default_timeout(self):
        loaded = load_config(self._config("""
            local-ydb:
              verified:
                workload: {type: kv, operation: upsert}
                load: {parameter: rate, values: [10]}
                measurement:
                  warmup: 1
                  duration: 2
                  repetitions: 1
                  verification-repetitions: 2
        """))
        configuration = loaded.runs[0]
        self.assertEqual(configuration.parameters["local_ydb"]["measurement"]["verification_repetitions"], 2)
        self.assertEqual(configuration.timeout_seconds, 300 + 3 * (1 + 2 + 10))

        invalid = self._config("""
            local-ydb:
              invalid:
                workload: {type: kv, operation: upsert}
                load: {parameter: rate, values: [10]}
                measurement: {verification-repetitions: -1}
        """)
        with self.assertRaisesRegex(BenchmarkError, "verification-repetitions"):
            load_config(invalid)

        too_many = self._config("""
            local-ydb:
              invalid:
                workload: {type: kv, operation: upsert}
                load: {parameter: rate, values: [10]}
                measurement: {verification-repetitions: 21}
        """)
        with self.assertRaisesRegex(BenchmarkError, "must be at most 20"):
            load_config(too_many)

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
        self.assertEqual(model["local_ydb_workloads"], local_ydb_workloads.web_workload_catalog())
        self.assertTrue(benchmark["builder_supported"])
        self.assertEqual(benchmark["profile_kind"], "local-ydb")
        self.assertEqual(profile["local_ydb"]["workload"]["type"], "stock")
        self.assertEqual(profile["local_ydb"]["geometry"]["max_dynamic_nodes"], 4)
        self.assertEqual(profile["local_ydb"]["load"]["values"], [10, 20])
        self.assertTrue(profile["local_ydb"]["load"]["allow_errors"])
        self.assertEqual(profile["local_ydb"]["measurement"]["verification_repetitions"], 0)
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

    def test_local_ydb_log_profile_defaults_validation_and_web_contract(self):
        loaded = load_config(self._config("""
            local-ydb:
              log-smoke:
                workload: {type: log, operation: bulk-upsert}
                load: {parameter: threads, values: [1, 2, 4]}
        """))
        profile = loaded.runs[0].parameters["local_ydb"]
        self.assertEqual(
            profile["workload"]["options"],
            {
                "min-partitions": 40,
                "max-partitions": 1000,
                "partition-size-mb": 2000,
                "auto-partition": 1,
                "store": "row",
                "ttl-minutes": 0,
                "string-length": 8,
                "integer-columns": 0,
                "string-columns": 0,
                "key-columns": 0,
                "rows-per-operation": 1,
                "null-percent": 10,
            },
        )
        definition = local_ydb_workloads.workload_definition("log")
        self.assertEqual((definition.dataset_scope, definition.warmup_mode), ("sample", "separate"))
        self.assertEqual(profile["load"]["values"], [1, 2, 4])

        model = web.editor_model(loaded, self.root / "results")
        editor_profile = model["profiles"][0]["local_ydb"]
        self.assertEqual(editor_profile["workload"]["type"], "log")
        self.assertEqual(editor_profile["load"]["parameter"], "threads")
        self.assertIn(
            "threads:{values:[1,2,4,8,16,32,64],start:1,maximum:256}",
            web._JS,
        )
        self.assertIn(
            "localYdbLoadForWorkload(config.load,parameters,nextDefinition,config.workload)",
            web._JS,
        )
        self.assertIn("log:'batches/s'", web._JS)
        transactions = next(metric for metric in LOCAL_YDB_BENCHMARK.metrics if metric.name == "transactions")
        throughput = next(metric for metric in LOCAL_YDB_BENCHMARK.metrics if metric.name == "throughput")
        self.assertEqual(transactions.unit, "operations")
        self.assertEqual(throughput.unit, "operations/s")

        invalid = (
            ({"max-partitions": 3, "min-partitions": 4}, "max-partitions.*must not be below"),
            (
                {"integer-columns": 1, "string-columns": 1, "key-columns": 3},
                "key-columns.*must not exceed",
            ),
            ({"null-percent": 101}, "null-percent.*must not exceed 100"),
            ({"rows-per-operation": 0}, "rows-per-operation.*positive integer"),
            ({"string-length": 0}, "string-length.*positive integer"),
            ({"store": "external"}, "store.*must be one of row, column"),
        )
        for options, message in invalid:
            with self.subTest(options=options), self.assertRaisesRegex(BenchmarkError, message):
                local_ydb_workloads.normalize_workload(
                    {"type": "log", "operation": "bulk-upsert", "options": options},
                    "workload",
                )

        invalid_load = self._config("""
            local-ydb:
              invalid:
                workload: {type: log, operation: insert}
                load: {parameter: rate, values: [1000]}
        """)
        with self.assertRaisesRegex(BenchmarkError, "load.parameter.*must be one of threads"):
            load_config(invalid_load)

    @unittest.skipUnless(shutil.which("node"), "node is required for the web Builder behavior test")
    def test_local_ydb_web_builder_resets_only_incompatible_load_parameters(self):
        start = web._JS.index("const localYdbLoadDefaults=")
        finish = web._JS.index("function defaultLocalYdbWorkload", start)
        unit_start = web._JS.index("function localSearchAxisLabel")
        unit_finish = web._JS.index("function localAttemptRows", unit_start)
        script = web._JS[start:finish] + web._JS[unit_start:unit_finish] + """
            const points=localYdbResetLoadParameter(
              {parameter:'rate',allow_errors:false,values:[1000]},'threads'
            );
            const automatic=localYdbResetLoadParameter({
              parameter:'rate',allow_errors:true,
              search:{start:777,maximum:9999,multiplier:3,resolution_percent:7},
              objective:{type:'latency-slo',percentile:'p99',max_ms:10}
            },'threads');
            const custom={
              parameter:'threads',allow_errors:false,
              search:{start:7,maximum:91,multiplier:4,resolution_percent:9},
              objective:{type:'maximize-throughput',target_role:'dynamic'}
            };
            const compatible=localYdbLoadForWorkload(custom,['rate','threads']);
            const tpccDefinition={
              load_parameters:['max-sessions'],default_client_threads:2,
              default_warmup_seconds:null,
              load_limits:{'max-sessions':{option:'warehouses',multiplier:10}}
            };
            const topicDefinition={
              load_parameters:['rate'],default_client_threads:1,default_warmup_seconds:10,load_limits:{}
            };
            const tpccWorkload={options:{warehouses:10}};
            const switchedToTpcc=localYdbLoadForWorkload(
              custom,tpccDefinition.load_parameters,tpccDefinition,tpccWorkload
            );
            const warehouseOnePoints=localYdbClampLoad(
              {...switchedToTpcc,values:[1,8,16,64]},tpccDefinition,{options:{warehouses:1}}
            );
            const warehouseOneSearch=localYdbClampLoad({
              parameter:'max-sessions',allow_errors:false,
              search:{start:32,maximum:100,multiplier:2,resolution_percent:2},
              objective:{type:'maximize-throughput',target_role:'dynamic'}
            },tpccDefinition,{options:{warehouses:1}});
            const switchedBack=localYdbLoadForWorkload(
              switchedToTpcc,['rate','threads'],{load_limits:{}},{options:{}}
            );
            process.stdout.write(JSON.stringify({
              points,automatic,compatible,same_reference:compatible===custom,
              switchedToTpcc,warehouseOnePoints,warehouseOneSearch,switchedBack,
              tpccThreads:localYdbDefaultClientThreads(tpccDefinition),
              topicThreads:localYdbDefaultClientThreads(topicDefinition),
              fallbackThreads:localYdbDefaultClientThreads({}),
              warmups:[tpccDefinition,topicDefinition,{}].map(localYdbDefaultWarmupSeconds),
              units:['kv','stock','log','topic','future'].map(localYdbThroughputUnit),
              axes:['kv','stock','topic'].map(workload=>localSearchAxisLabel('rate',workload))
            }));
        """
        completed = subprocess.run(
            [shutil.which("node"), "-e", script],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        result = json.loads(completed.stdout)
        self.assertEqual(
            result["points"],
            {
                "parameter": "threads",
                "allow_errors": False,
                "values": [1, 2, 4, 8, 16, 32, 64],
            },
        )
        self.assertEqual(
            result["automatic"],
            {
                "parameter": "threads",
                "allow_errors": True,
                "search": {"start": 1, "maximum": 256, "multiplier": 3, "resolution_percent": 7},
                "objective": {"type": "latency-slo", "percentile": "p99", "max_ms": 10},
            },
        )
        self.assertTrue(result["same_reference"])
        self.assertEqual(
            result["compatible"],
            {
                "parameter": "threads",
                "allow_errors": False,
                "search": {"start": 7, "maximum": 91, "multiplier": 4, "resolution_percent": 9},
                "objective": {"type": "maximize-throughput", "target_role": "dynamic"},
            },
        )
        self.assertEqual(
            result["switchedToTpcc"],
            {
                "parameter": "max-sessions",
                "allow_errors": False,
                "search": {"start": 1, "maximum": 100, "multiplier": 4, "resolution_percent": 9},
                "objective": {"type": "maximize-throughput", "target_role": "dynamic"},
            },
        )
        self.assertEqual(result["warehouseOnePoints"]["values"], [1, 8, 10])
        self.assertEqual(result["warehouseOneSearch"]["search"]["start"], 10)
        self.assertEqual(result["warehouseOneSearch"]["search"]["maximum"], 10)
        self.assertEqual(result["switchedBack"]["parameter"], "rate")
        self.assertEqual(result["switchedBack"]["search"]["start"], 1000)
        self.assertEqual(result["switchedBack"]["search"]["maximum"], 100000)
        self.assertEqual(result["tpccThreads"], 2)
        self.assertEqual(result["topicThreads"], 1)
        self.assertEqual(result["fallbackThreads"], 64)
        self.assertEqual(result["warmups"], [None, 10, 10])
        self.assertEqual(
            result["units"],
            ["requests/s", "transactions/s", "batches/s", "messages/s", "operations/s"],
        )
        self.assertEqual(
            result["axes"],
            ["Offered rate (requests/s)", "Offered rate (transactions/s)", "Offered rate (messages/s)"],
        )

    @unittest.skipUnless(shutil.which("node"), "node is required for the web Builder warmup test")
    def test_local_ydb_web_builder_omits_automatic_tpcc_warmup(self):
        start = web._JS.index("function yamlArray")
        finish = web._JS.index("async function syncEditor", start)
        comparison_start = web._JS.index("function localComparisonConfig")
        comparison_finish = web._JS.index("function localComparisonSemantic", comparison_start)
        script = (
            """
            const editor={model:{local_ydb_workloads:[]}};
            const document={querySelector:()=>({value:''})};
            function localInteger(){throw Error('blank automatic warmup must not be parsed as an integer')}
        """
            + web._JS[start:finish]
            + web._JS[comparison_start:comparison_finish]
            + """
            const tpcc={default_warmup_seconds:null};
            const kv={default_warmup_seconds:10};
            function profile(warmup){return {timeout:null,local_ydb:{
              workload:{type:'tpcc',operation:'run',options:{warehouses:10}},
              geometry:{preset:'single',static_nodes:1,dynamic_nodes:1,max_dynamic_nodes:1,disk_size_gb:64,storage_groups:1},
              client:{threads:2},load:{parameter:'max-sessions',allow_errors:false,values:[1]},
              measurement:{warmup,duration:2,repetitions:1,verification_repetitions:0},
              affinity:{ydb_cli:{mode:'none',cpus:null},static_nodes:{mode:'none',cpus:null},dynamic_nodes:{mode:'none',cpus:null}}
            }}}
            const automatic=[];serializeLocalYdb(automatic,profile(null));
            const explicit=[];serializeLocalYdb(explicit,profile(0));
            process.stdout.write(JSON.stringify({
              automatic:automatic.join('\\n'),explicit:explicit.join('\\n'),
              blankTpcc:localYdbWarmupInput('warmup',tpcc),
              blankKv:localYdbWarmupInput('warmup',kv),
              comparisonAutomatic:localComparisonConfig({parameters:{measurement:{warmup:null}}})['Warmup seconds'],
              comparisonMissing:localComparisonConfig({parameters:{measurement:{}}})['Warmup seconds']
            }));
        """
        )
        completed = subprocess.run(
            [shutil.which("node"), "-e", script],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        result = json.loads(completed.stdout)
        self.assertNotIn("warmup:", result["automatic"])
        self.assertIn("warmup: 0", result["explicit"])
        self.assertIsNone(result["blankTpcc"])
        self.assertEqual(result["blankKv"], 10)
        self.assertEqual(result["comparisonAutomatic"], "automatic")
        self.assertEqual(result["comparisonMissing"], "—")
        self.assertIn("config.measurement.warmup=localYdbDefaultWarmupSeconds(nextDefinition)", web._JS)

    @unittest.skipUnless(shutil.which("node"), "node is required for the local YDB validity UI test")
    def test_local_ydb_web_hides_latency_for_empty_measurements(self):
        result_start = web._JS.index("function localResultMetrics")
        result_finish = web._JS.index("function localVerificationCount", result_start)
        attempt_start = web._JS.index("function localLegacyResultSchema")
        attempt_finish = web._JS.index("function localChart", attempt_start)
        script = web._JS[result_start:result_finish] + web._JS[attempt_start:attempt_finish] + """
            const empty={attempt:1,empty_repetitions:1,throughput:123,errors:7,p99_ms:0};
            const valid={attempt:2,empty_repetitions:0,throughput:100,errors:1,p99_ms:9};
            const schema=localLegacyResultSchema('kv');
            const rows=localAttemptRows([empty,valid],schema);
            const holdout=localResultMetrics({
              metrics_source:'verification',
              verified_metrics:{empty_repetitions:1,throughput:123,errors:7,p99_ms:0}
            },schema);
            process.stdout.write(JSON.stringify({
              empty_label:localAttemptMetric(empty,'p99_ms',schema),
              valid_label:localAttemptMetric(valid,'p99_ms',schema),
              empty_chart:rows.get('1'),valid_chart:rows.get('2'),holdout
            }));
        """
        completed = subprocess.run(
            [shutil.which("node"), "-e", script],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        result = json.loads(completed.stdout)
        self.assertEqual(result["empty_label"], "—")
        self.assertEqual(result["valid_label"], 9)
        self.assertIsNone(result["empty_chart"]["p99_ms"])
        self.assertEqual(result["empty_chart"]["throughput"], 123)
        self.assertEqual(result["empty_chart"]["errors"], 7)
        self.assertEqual(result["valid_chart"]["p99_ms"], 9)
        self.assertIsNone(result["holdout"]["metrics"]["p99_ms"])
        self.assertEqual(result["holdout"]["metrics"]["throughput"], 123)

    @unittest.skipUnless(shutil.which("node"), "node is required for the schema-aware local YDB UI test")
    def test_local_ydb_web_uses_custom_schema_units_slo_and_identity(self):
        result_start = web._JS.index("function localResultMetrics")
        result_finish = web._JS.index("function localVerificationCount", result_start)
        stable_start = web._JS.index("function localComparisonStable")
        stable_finish = result_start
        schema_start = web._JS.index("function localLegacyResultSchema")
        schema_finish = web._JS.index("function localChart", schema_start)
        number_start = web._JS.index("function chartNumber")
        number_finish = web._JS.index("function chartSeriesLabel", number_start)
        script = (
            web._JS[stable_start:stable_finish]
            + web._JS[result_start:result_finish]
            + web._JS[schema_start:schema_finish]
            + web._JS[number_start:number_finish]
            + """
            const customSchema={
              schema_id:'fake-json-v1',throughput_unit:'widgets/s',reports_errors:false,
              metrics:[
                {name:'throughput',unit:'widgets/s',required:true,repetition_aggregation:'median'},
                {name:'latency_ms',unit:'ms',required:true,repetition_aggregation:'median'},
                {name:'queue_depth',unit:'messages',required:false,repetition_aggregation:'median'}
              ],
              slo_metrics:{p90:'latency_ms'}
            };
            const parameters={workload:{type:'kv',operation:'upsert'},load:{parameter:'rate',objective:{type:'latency-slo',percentile:'p90'}}};
            const custom={parameters,workload_result_schema:customSchema};
            const changed={parameters,workload_result_schema:{...customSchema,schema_id:'fake-json-v2'}};
            const legacy={parameters:{workload:{type:'kv',operation:'upsert'},load:{parameter:'rate',objective:{type:'points'}}}};
            const explicitLegacy={...legacy,workload_result_schema:localLegacyResultSchema('kv')};
            const empty=localResultMetrics({selected_metrics:{empty_repetitions:1,throughput:4,latency_ms:0,queue_depth:8}},customSchema);
            const genericSchema=localLegacyResultSchema('kv');
            process.stdout.write(JSON.stringify({
              slo:localSloMetric(customSchema,'p90'),unit:localYdbThroughputUnit(custom),
              metrics:localDisplayedMetrics(customSchema).map(item=>item.name),empty:empty.metrics,
              generic_table:localDisplayedMetrics(genericSchema,{type:'latency-slo',percentile:'p95'}).map(
                item=>[item.name,localMetricLabel(genericSchema,item.name)]
              ),
              generic_curves:localComparisonCurveMetrics(
                genericSchema,{type:'latency-slo',percentile:'p95'}
              ).map(item=>item.name),
              custom_identity:localComparisonSemantic(custom),changed_identity:localComparisonSemantic(changed),
              legacy_identity:localComparisonSemantic(legacy),explicit_legacy_identity:localComparisonSemantic(explicitLegacy),
              empty_number:Number.isFinite(chartNumber('   '))
            }));
            """
        )
        completed = subprocess.run(
            [shutil.which("node"), "-e", script],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        result = json.loads(completed.stdout)
        self.assertEqual(result["slo"], "latency_ms")
        self.assertEqual(result["unit"], "widgets/s")
        self.assertEqual(result["metrics"], ["throughput", "latency_ms", "queue_depth"])
        self.assertEqual(
            result["generic_table"],
            [["throughput", "Throughput"], ["p95_ms", "p95"], ["errors", "Errors"]],
        )
        self.assertEqual(result["generic_curves"], ["throughput", "p95_ms", "errors"])
        self.assertIsNone(result["empty"]["latency_ms"])
        self.assertEqual(result["empty"]["queue_depth"], 8)
        self.assertNotEqual(result["custom_identity"], result["changed_identity"])
        self.assertEqual(result["legacy_identity"], result["explicit_legacy_identity"])
        self.assertFalse(result["empty_number"])

    @unittest.skipUnless(shutil.which("node"), "node is required for the local YDB attempts UI test")
    def test_local_ydb_web_attempts_use_objective_latency_percentile(self):
        schema_start = web._JS.index("function localLegacyResultSchema")
        schema_finish = web._JS.index("function localChart", schema_start)
        render_start = web._JS.index("function renderLocalYdbProfile")
        render_finish = web._JS.index("async function mountLocalYdbProfile", render_start)
        script = (
            """
            const esc=value=>String(value??'');
            const metricLabel=value=>String(value??'—');
            const elapsedLabel=value=>String(value??0);
            const localElapsed=()=>0;
            const localPhaseLabel=value=>value||'';
            const localActivityLog=()=>'';
            const localRestoreActivityScroll=()=>{};
            const localProfileDetails=()=>'';
            const localVerificationSummary=()=>'';
            const localKpi=()=>'';
            const localOutcomeLabel=value=>value||'';
            const localSearchAxisLabel=value=>value||'load';
            const localBestRows=(attempts,objective,xField)=>new Map(
              attempts.map(item=>[String(item[xField]),item])
            );
            const chartColors=[];
            const localChart=()=>'';
            const localCommandDetails=()=>'';
            const bindChartTooltips=()=>{};
            """
            + web._JS[schema_start:schema_finish]
            + web._JS[render_start:render_finish]
            + """
            const schema={
              schema_id:'generic-total-v1',throughput_unit:'requests/s',reports_errors:true,
              metrics:[
                {name:'throughput',unit:'requests/s',repetition_aggregation:'median'},
                {name:'errors',unit:'errors',repetition_aggregation:'sum'},
                {name:'p95_ms',unit:'ms',repetition_aggregation:'median'},
                {name:'p99_ms',unit:'ms',repetition_aggregation:'median'}
              ],
              slo_metrics:{p95:'p95_ms',p99:'p99_ms'}
            };
            const container={
              dataset:{},innerHTML:'',querySelector:()=>null,querySelectorAll:()=>[]
            };
            renderLocalYdbProfile(container,{
              state:'passed',started_at:'2025-01-01T00:00:00Z',progress:{search_stage:1},searches:[],
              parameters:{
                workload:{type:'kv'},
                load:{parameter:'rate',objective:{type:'latency-slo',percentile:'p95',max_ms:10}}
              },
              workload_result_schema:schema,
              attempts:[{
                attempt:1,search_stage:1,dynamic_nodes:1,load:90,throughput:80,
                p95_ms:5.95,p99_ms:99.99,errors:0,empty_repetitions:0,
                static_cpu_mean:10,dynamic_cpu_mean:20,cli_cpu_mean:30,
                passed:true,decision:'within SLO',duration_seconds:1
              }]
            });
            const table=container.innerHTML.slice(container.innerHTML.indexOf('<table class=local-attempts>'));
            process.stdout.write(JSON.stringify({table}));
            """
        )
        completed = subprocess.run(
            [shutil.which("node"), "-e", script],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        table = json.loads(completed.stdout)["table"]
        self.assertIn(">p95 (ms)</th>", table)
        self.assertNotIn(">p99 (ms)</th>", table)
        self.assertIn(">5.95</td>", table)
        self.assertNotIn(">99.99</td>", table)

    @unittest.skipUnless(shutil.which("node"), "node is required for the schema-aware Builder test")
    def test_local_ydb_web_builder_omits_unsupported_error_controls(self):
        start = web._JS.index("function localField")
        finish = web._JS.index("function localNumber", start)
        script = (
            """
            const esc=value=>String(value??'');
            const localYdbGeometryKeys={static_nodes:'static-nodes',dynamic_nodes:'dynamic-nodes',max_dynamic_nodes:'max-dynamic-nodes',disk_size_gb:'disk-size-gb',storage_groups:'storage-groups'};
            const localYdbAffinityKeys={ydb_cli:'ydb-cli',static_nodes:'static-nodes',dynamic_nodes:'dynamic-nodes'};
            const definition={type:'fake',operations:['run'],load_parameters:['rate'],options:[],slo_metrics:{p90:'latency_ms'},reports_errors:false};
            const editor={model:{local_ydb_workloads:[definition],affinity_modes:['none'],benchmarks:[{name:'local-ydb'}]}};
            function localYdbWorkloadDefinition(){return definition}
            function localYdbDefaultWarmupSeconds(){return 10}
        """
            + web._JS[start:finish]
            + """
            const profile={benchmark:'local-ydb',name:'custom',timeout:null,local_ydb:{
              workload:{type:'fake',operation:'run',options:{}},
              geometry:{preset:'single',static_nodes:1,dynamic_nodes:1,max_dynamic_nodes:1,disk_size_gb:1,storage_groups:1},
              client:{threads:1},
              load:{parameter:'rate',allow_errors:false,search:{start:1,maximum:10,multiplier:2,resolution_percent:2},objective:{type:'latency-slo',percentile:'p90',max_ms:5,max_errors:0,min_achieved_rate_ratio:.9}},
              measurement:{warmup:0,duration:1,repetitions:1,verification_repetitions:0},
              affinity:{ydb_cli:{mode:'none',cpus:null},static_nodes:{mode:'none',cpus:null},dynamic_nodes:{mode:'none',cpus:null}}
            }};
            const html=localYdbProfileEditor(profile);
            process.stdout.write(JSON.stringify({
              error_toggle:html.includes('local-load-allow-errors'),error_limit:html.includes('local-slo-max-errors'),
              p90:html.includes('value="p90"'),p99_default:localYdbSloPercentile({slo_metrics:{p50:'x',p99:'y'}},'missing'),
              first_default:localYdbSloPercentile({slo_metrics:{p90:'x'}},'missing')
            }));
        """
        )
        completed = subprocess.run(
            [shutil.which("node"), "-e", script],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        result = json.loads(completed.stdout)
        self.assertFalse(result["error_toggle"])
        self.assertFalse(result["error_limit"])
        self.assertTrue(result["p90"])
        self.assertEqual(result["p99_default"], "p99")
        self.assertEqual(result["first_default"], "p90")
        self.assertIn("document.querySelector('#local-load-allow-errors')?.checked", web._JS)
        self.assertIn("workloadDefinition.reports_errors?localInteger('local-slo-max-errors',0):0", web._JS)
        self.assertIn("config.load.objective.max_errors=0", web._JS)
        self.assertIn("Applied to both producer and consumer thread counts", web._JS)

    def test_local_ydb_stock_commands_do_not_use_kv_path_option(self):
        cli_context = local_ydb_workloads.WorkloadCli(
            Path("ydb"),
            "grpc://host.example:2135",
            "/Root/bench",
        )
        workload = local_ydb_workloads.normalize_workload(
            {
                "type": "stock",
                "operation": "add-rand-order",
                "options": {
                    "products": 10,
                    "quantity": 100,
                    "orders": 0,
                    "min-partitions": 1,
                    "auto-partition": 0,
                },
            },
            "workload",
        )
        command = local_ydb_workloads.build_init_argv(
            cli_context,
            "ignored-table-prefix",
            workload,
        )
        self.assertNotIn("--path", command)
        self.assertNotIn("ignored-table-prefix", command)
        self.assertEqual(local_ydb_workloads.workload_table_path("stock", "ignored-table-prefix"), "stock")
        self.assertNotIn(
            "--path",
            local_ydb_workloads.build_clean_argv(cli_context, "ignored-table-prefix", "stock"),
        )

        workload["options"]["limit"] = 5
        add_command = local_ydb_workloads.build_run_argv(
            cli_context,
            "ignored-table-prefix",
            workload,
            "threads",
            8,
            30,
            64,
        )
        put_command = local_ydb_workloads.build_run_argv(
            cli_context,
            "ignored-table-prefix",
            {**workload, "operation": "put-rand-order"},
            "threads",
            8,
            30,
            64,
        )
        self.assertEqual(add_command[add_command.index("run") + 1], "add-rand-order")
        self.assertEqual(put_command[put_command.index("run") + 1], "put-rand-order")
        self.assertEqual(add_command[add_command.index("--threads") + 1], 8)

    def test_local_ydb_log_commands_are_golden_and_use_threads_as_load(self):
        cli_context = local_ydb_workloads.WorkloadCli(
            Path("/tmp/ydb cli"),
            "grpc://benchmark-host.example:2135",
            "/Root/bench",
        )
        workload = local_ydb_workloads.normalize_workload(
            {
                "type": "log",
                "operation": "bulk-upsert",
                "options": {
                    "min-partitions": 1,
                    "max-partitions": 4,
                    "partition-size-mb": 512,
                    "auto-partition": 0,
                    "store": "column",
                    "ttl-minutes": 0,
                    "string-length": 16,
                    "integer-columns": 1,
                    "string-columns": 2,
                    "key-columns": 3,
                    "rows-per-operation": 50,
                    "null-percent": 0,
                },
            },
            "workload",
        )
        base = [
            Path("/tmp/ydb cli"),
            "--endpoint",
            "grpc://benchmark-host.example:2135",
            "--database",
            "/Root/bench",
            "workload",
            "log",
            "--path",
            "log-table",
        ]
        self.assertEqual(
            local_ydb_workloads.build_init_argv(cli_context, "log-table", workload),
            base
            + [
                "init",
                "--min-partitions",
                1,
                "--max-partitions",
                4,
                "--partition-size",
                512,
                "--auto-partition",
                0,
                "--len",
                16,
                "--int-cols",
                1,
                "--str-cols",
                2,
                "--key-cols",
                3,
                "--ttl",
                0,
                "--store",
                "column",
                "--null-percent",
                0,
            ],
        )
        expected_run = base + [
            "run",
            "bulk-upsert",
            "--seconds",
            30,
            "--threads",
            32,
            "--quiet",
            "--rows",
            50,
            "--len",
            16,
            "--int-cols",
            1,
            "--str-cols",
            2,
            "--key-cols",
            3,
            "--null-percent",
            0,
        ]
        self.assertEqual(
            local_ydb_workloads.build_run_argv(cli_context, "log-table", workload, "threads", 32, 30, 64),
            expected_run,
        )
        self.assertNotIn("--rate", expected_run)
        self.assertEqual(expected_run[expected_run.index("--rows") + 1], 50)
        self.assertEqual(expected_run[expected_run.index("--threads") + 1], 32)
        for operation in ("insert", "upsert", "bulk-upsert"):
            with self.subTest(operation=operation):
                command = local_ydb_workloads.build_run_argv(
                    cli_context,
                    "log-table",
                    {**workload, "operation": operation},
                    "threads",
                    2,
                    1,
                    64,
                )
                self.assertEqual(command[command.index("run") + 1], operation)
                self.assertNotIn("bulk_upsert", command)
        self.assertEqual(
            local_ydb_workloads.build_clean_argv(cli_context, "log-table", "log"),
            base + ["clean"],
        )
        with self.assertRaisesRegex(BenchmarkError, "log does not support load parameter rate"):
            local_ydb_workloads.build_run_argv(cli_context, "log-table", workload, "rate", 1000, 30, 64)

    def test_local_ydb_log_uses_sample_lifecycle_and_preserves_rows_per_operation(self):
        workload = local_ydb_workloads.normalize_workload(
            {
                "type": "log",
                "operation": "bulk-upsert",
                "options": {"rows-per-operation": 25},
            },
            "workload",
        )
        metrics_output = """
            Total Txs Txs/Sec Retries Errors p50(ms) p95(ms) p99(ms) pMax(ms)
            10 10 0 0 1 2 3 4
        """
        cluster = mock.Mock(
            ydb_cli=Path("/tmp/ydb"),
            client_endpoint="grpc://benchmark-host:2135",
            database="/Root/bench",
            static_pids=(10,),
            dynamic_pids=(20,),
        )
        cluster.init_workload.side_effect = lambda command, **_kwargs: (
            self._local_ydb_command_result(command),
            [self._local_ydb_command_result(command)],
        )
        cluster._run.side_effect = lambda command, **_kwargs: self._local_ydb_command_result(command)
        monitor = mock.Mock(records=[])
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
        topology = CpuTopology(
            allowed_cpus=(0,),
            numa_nodes=((0, (0,)),),
            chiplets=(),
            physical_cores=((0,),),
        )
        progress = []
        with mock.patch.object(local_ydb, "LinuxCpuMonitor", return_value=monitor), mock.patch.object(
            local_ydb, "atomic_write_text"
        ), mock.patch.object(local_ydb, "atomic_write_json"), mock.patch.object(
            local_ydb,
            "run_command",
            side_effect=lambda command, *_args, **_kwargs: self._local_ydb_command_result(command, metrics_output),
        ):
            lifecycle = local_ydb.WorkloadLifecycle(
                cluster,
                local_ydb_workloads.WorkloadCli(cluster.ydb_cli, cluster.client_endpoint, cluster.database),
                workload,
                {"parameter": "threads"},
                {"warmup": 1, "duration": 1},
                64,
                LOCAL_YDB_BENCHMARK,
                topology,
                {"ydb_cli": None, "static_nodes": None, "dynamic_nodes": None},
                None,
                lambda phase, **fields: progress.append({"phase": phase, **fields}),
            )
            lifecycle.open_profile(self.root / "log-profile", "ignored-profile-table")
            _metrics, commands = lifecycle.run_sample(
                2,
                1,
                1,
                1,
                self.root / "log-lifecycle",
                "log-table",
                {"attempt": 1},
            )
            lifecycle.close_profile()

        self.assertEqual(
            [command["phase"] for command in commands],
            ["initializing-workload", "warming-up", "measuring", "cleaning-workload"],
        )
        self.assertEqual(
            [item["phase"] for item in progress],
            ["initializing-workload", "warming-up", "measuring", "cleaning-workload"],
        )
        init, warmup, measure, clean = (command["argv"] for command in commands)
        self.assertIn("init", init)
        self.assertIn("clean", clean)
        self.assertEqual(init[init.index("--path") + 1], clean[clean.index("--path") + 1])
        for command in (warmup, measure):
            self.assertEqual(command[command.index("run") + 1], "bulk-upsert")
            self.assertEqual(command[command.index("--threads") + 1], "2")
            self.assertEqual(command[command.index("--rows") + 1], "25")
            self.assertNotIn("--rate", command)

    def test_local_ydb_topic_uses_fresh_sample_dataset_and_inline_cpu_window(self):
        workload = local_ydb_workloads.normalize_workload(
            {"type": "topic", "operation": "full", "options": {"partitions": 4, "consumers": 2}},
            "workload",
        )
        output = """
1 0 0 0 0 0 0 0 0 0 2026-08-25T10:00:11Z
2 120 1 4 7 8 9 180 2 12 2026-08-25T10:00:12Z
3 120 1 4 7 8 9 180 2 12 2026-08-25T10:00:13Z
Total 999 999 999 999 999 999 999 999 999 2026-08-25T10:00:14Z
"""
        cluster = mock.Mock(
            ydb_cli=Path("/tmp/ydb"),
            client_endpoint="grpc://benchmark-host:2135",
            database="/Root/bench",
            static_pids=(10,),
            dynamic_pids=(20,),
        )
        cluster.init_workload.side_effect = lambda command, **_kwargs: (
            self._local_ydb_command_result(command),
            [self._local_ydb_command_result(command)],
        )
        cluster._run.side_effect = lambda command, **_kwargs: self._local_ydb_command_result(command)
        monitor = mock.Mock(records=[])
        monitor.stop.return_value = {}
        monitor.summary.return_value = {
            "static_cpu_mean": 1,
            "dynamic_cpu_mean": 2,
            "cli_cpu_mean": 3,
            "host_cpu_mean": 4,
        }
        topology = CpuTopology(
            allowed_cpus=(0,),
            numa_nodes=((0, (0,)),),
            chiplets=(),
            physical_cores=((0,),),
        )
        progress = []
        with mock.patch.object(local_ydb, "LinuxCpuMonitor", return_value=monitor), mock.patch.object(
            local_ydb,
            "run_command",
            side_effect=lambda command, *_args, **_kwargs: self._local_ydb_command_result(command, output),
        ) as execute:
            lifecycle = local_ydb.WorkloadLifecycle(
                cluster,
                local_ydb_workloads.WorkloadCli(cluster.ydb_cli, cluster.client_endpoint, cluster.database),
                workload,
                {"parameter": "rate", "allow_errors": False},
                {"warmup": 1, "duration": 2},
                2,
                LOCAL_YDB_BENCHMARK,
                topology,
                {"ydb_cli": None, "static_nodes": None, "dynamic_nodes": None},
                None,
                lambda phase, **fields: progress.append({"phase": phase, **fields}),
            )
            lifecycle.open_profile(self.root / "topic-profile", "ignored-profile-topic")
            first_metrics, first_commands = lifecycle.run_sample(
                150,
                1,
                1,
                2,
                self.root / "topic-repeat-1",
                "topic-sample-1",
                {"attempt": 1},
            )
            second_metrics, second_commands = lifecycle.run_sample(
                150,
                1,
                2,
                2,
                self.root / "topic-repeat-2",
                "topic-sample-2",
                {"attempt": 1},
                purpose="verification",
            )
            lifecycle.close_profile()

        self.assertEqual(cluster.init_workload.call_count, 2)
        self.assertEqual(execute.call_count, 2)
        self.assertEqual(cluster._run.call_count, 2)
        self.assertEqual([len(first_commands), len(second_commands)], [3, 3])
        self.assertEqual(
            [command["phase"] for command in first_commands],
            ["initializing-workload", "measuring", "cleaning-workload"],
        )
        self.assertEqual(
            [command["phase"] for command in second_commands],
            ["verification-initializing", "verification-measuring", "verification-cleanup"],
        )
        for commands, topic_path in ((first_commands, "topic-sample-1"), (second_commands, "topic-sample-2")):
            for command in commands:
                argv = command["argv"]
                self.assertEqual(argv[argv.index("--topic") + 1], topic_path)
            run = commands[1]["argv"]
            self.assertEqual(run[run.index("--seconds") + 1], "3")
            self.assertEqual(run[run.index("--warmup") + 1], "1")
            self.assertEqual(run[run.index("--producer-threads") + 1], "2")
            self.assertEqual(run[run.index("--consumer-threads") + 1], "2")
        self.assertEqual([call.args[2] for call in execute.call_args_list], [33, 33])
        self.assertTrue(all(call.kwargs["timeout"] == 120 for call in cluster._run.call_args_list))
        self.assertTrue(all(call.kwargs["ignore_cancellation"] for call in cluster._run.call_args_list))
        self.assertEqual(first_metrics["throughput"], 90)
        self.assertEqual(second_metrics["transactions"], 180)
        self.assertEqual(
            monitor.summary.call_args_list,
            [
                mock.call(started_at_unix=1787652011.0, finished_at_unix=1787652013.0),
                mock.call(started_at_unix=1787652011.0, finished_at_unix=1787652013.0),
            ],
        )
        self.assertEqual(
            [item["phase"] for item in progress],
            [
                "initializing-workload",
                "measuring",
                "cleaning-workload",
                "verification-initializing",
                "verification-measuring",
                "verification-cleanup",
            ],
        )

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
                measurement: {warmup: 1, duration: 1, repetitions: 1, verification-repetitions: 2}
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
        cluster.init_workload.side_effect = lambda command, **_kwargs: (
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

        def measurement_stdout(throughput):
            return """
                Total Txs Txs/Sec Retries Errors p50(ms) p95(ms) p99(ms) pMax(ms)
                1 {throughput} {throughput} 0 0 1 2 3 4
            """.format(throughput=throughput)

        measurement_outputs = iter(measurement_stdout(value) for value in (10, 20, 30))
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
            side_effect=lambda command, *_args, **_kwargs: command_result(command, next(measurement_outputs)),
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
        self.assertIn("verification-evaluating", [item["phase"] for item in progress])
        evaluating = next(item for item in progress if item["phase"] == "verification-evaluating")
        self.assertEqual(
            set(evaluating["verification"]),
            {"status", "configured_repetitions", "completed_repetitions"},
        )
        verification_completed = next(item for item in progress if item["phase"] == "verification-completed")
        for item in (evaluating, verification_completed):
            payload = json.dumps(item)
            self.assertNotIn('"samples"', payload)
            self.assertNotIn('"commands"', payload)
        self.assertEqual(len(manifest["attempts"]), 1)
        self.assertEqual(manifest["result"]["metrics_source"], "verification")
        self.assertEqual(manifest["result"]["selected_metrics"]["throughput"], 10)
        self.assertEqual(manifest["attempts"][0]["throughput"], 10)
        self.assertEqual(manifest["searches"][0]["selected_metrics"]["throughput"], 10)
        self.assertEqual(manifest["result"]["verified_metrics"]["throughput"], 25)
        self.assertTrue(manifest["result"]["holdout_accepted"])
        self.assertEqual(manifest["verification"]["completed_repetitions"], 2)
        self.assertTrue(manifest["verification"]["accepted"])
        self.assertEqual(manifest["verification"]["evaluation_kind"], "validity")
        self.assertNotIn("samples", manifest["verification"])
        self.assertTrue((output / "verification-repetitions.csv").is_file())
        self.assertTrue((output / "verification-summary.csv").is_file())
        with (output / "repetitions.csv").open(newline="", encoding="utf-8") as stream:
            search_rows = list(csv.DictReader(stream))
        with (output / "verification-repetitions.csv").open(newline="", encoding="utf-8") as stream:
            verification_rows = list(csv.DictReader(stream))
        with (output / "verification-summary.csv").open(newline="", encoding="utf-8") as stream:
            verification_summary = list(csv.DictReader(stream))
        self.assertEqual([float(row["throughput"]) for row in search_rows], [10])

        self.assertEqual([float(row["throughput"]) for row in verification_rows], [20, 30])
        self.assertNotIn("passed", verification_rows[0])
        self.assertEqual(verification_summary[0]["samples"], "2")
        self.assertEqual(float(verification_summary[0]["median_throughput"]), 25)
        verification_commands = []
        for repetition in (1, 2):
            commands_path = output / "verification" / "repeat-{:03d}".format(repetition) / "commands.json"
            verification_commands.extend(json.loads(commands_path.read_text(encoding="utf-8")))
        self.assertEqual(len(verification_commands) + len(commands), 12)
        self.assertEqual(
            [item["phase"] for item in verification_commands[:4]],
            [
                "verification-initializing",
                "verification-warmup",
                "verification-measuring",
                "verification-cleanup",
            ],
        )
        artifacts = next(event["artifacts"] for event in events if event["type"] == "step-artifacts")
        self.assertIn("verification-repetitions.csv", artifacts)

    def test_local_ydb_only_explicit_profile_timeout_caps_workload_commands(self):
        def configuration(name, timeout=""):
            return load_config(
                self._config(
                    """
                    local-ydb:
                      {name}:
                        {timeout}
                        workload: {{type: kv, operation: upsert}}
                        load: {{parameter: threads, values: [1]}}
                        measurement: {{warmup: 0, duration: 1, repetitions: 1}}
                        affinity:
                          ydb-cli: {{mode: none}}
                          static-nodes: {{mode: none}}
                          dynamic-nodes: {{mode: none}}
                    """.format(name=name, timeout=timeout),
                    name + ".yaml",
                )
            ).runs[0]

        original_lifecycle = local_ydb.WorkloadLifecycle
        command_timeouts = []

        def lifecycle(*args, **kwargs):
            command_timeouts.append(kwargs.get("command_timeout_seconds"))
            return original_lifecycle(*args, **kwargs)

        with mock.patch.object(local_ydb, "WorkloadLifecycle", side_effect=lifecycle):
            self._run_mock_local_ydb(
                configuration("default-timeout"),
                "default-timeout",
                [{"throughput": 1}],
            )
            self._run_mock_local_ydb(
                configuration("explicit-timeout", "timeout: 3"),
                "explicit-timeout",
                [{"throughput": 1}],
            )

        self.assertEqual(command_timeouts, [None, 3])

    def test_local_ydb_disabled_verification_keeps_search_metrics(self):
        configuration = load_config(self._config("""
            local-ydb:
              search-only:
                workload: {type: kv, operation: upsert}
                load: {parameter: threads, values: [1]}
                measurement: {warmup: 0, duration: 1, repetitions: 1}
                affinity:
                  ydb-cli: {mode: none}
                  static-nodes: {mode: none}
                  dynamic-nodes: {mode: none}
        """)).runs[0]
        manifest, output, events = self._run_mock_local_ydb(
            configuration,
            "verification-disabled",
            [{"throughput": 10}],
        )
        self.assertEqual(manifest["verification"]["status"], "disabled")
        self.assertEqual(manifest["result"]["metrics_source"], "search")
        self.assertNotIn("verified_metrics", manifest["result"])
        self.assertFalse((output / "verification-repetitions.csv").exists())
        artifacts = next(event["artifacts"] for event in events if event["type"] == "step-artifacts")
        self.assertNotIn("verification-repetitions.csv", artifacts)

    def test_local_ydb_verification_is_skipped_without_feasible_load(self):
        configuration = load_config(self._config("""
            local-ydb:
              no-feasible:
                workload: {type: kv, operation: upsert}
                load: {parameter: threads, values: [1]}
                measurement: {warmup: 0, duration: 1, repetitions: 1, verification-repetitions: 2}
                affinity:
                  ydb-cli: {mode: none}
                  static-nodes: {mode: none}
                  dynamic-nodes: {mode: none}
        """)).runs[0]
        manifest, output, _events = self._run_mock_local_ydb(
            configuration,
            "verification-skipped",
            [{"throughput": 10, "errors": 1}],
        )
        self.assertIsNone(manifest["result"]["selected_load"])
        self.assertEqual(manifest["result"]["metrics_source"], "search")
        self.assertEqual(manifest["verification"]["status"], "skipped")
        self.assertIn("did not select", manifest["verification"]["reason"])
        self.assertFalse((output / "verification-repetitions.csv").exists())
        self.assertTrue((output / "repetitions.csv").is_file())

    def test_local_ydb_latency_verification_reports_failed_holdout_without_changing_search(self):
        configuration = load_config(self._config("""
            local-ydb:
              latency:
                workload: {type: kv, operation: upsert}
                load:
                  parameter: threads
                  search: {start: 10, maximum: 10}
                  objective: {type: latency-slo, percentile: p99, max-ms: 10}
                measurement: {warmup: 0, duration: 1, repetitions: 1, verification-repetitions: 2}
                affinity:
                  ydb-cli: {mode: none}
                  static-nodes: {mode: none}
                  dynamic-nodes: {mode: none}
        """)).runs[0]
        manifest, _output, _events = self._run_mock_local_ydb(
            configuration,
            "verification-latency-fail",
            [
                {"throughput": 10, "p99_ms": 9},
                {"throughput": 11, "p99_ms": 9},
                {"throughput": 12, "p99_ms": 13},
            ],
        )
        self.assertEqual(manifest["state"], "passed")
        self.assertEqual(manifest["result"]["selected_metrics"]["p99_ms"], 9)
        self.assertEqual(manifest["result"]["verified_metrics"]["p99_ms"], 11)
        self.assertEqual(manifest["result"]["metrics_source"], "verification")
        self.assertFalse(manifest["result"]["holdout_accepted"])
        self.assertFalse(manifest["verification"]["accepted"])
        self.assertEqual(manifest["verification"]["evaluation_kind"], "objective")
        self.assertIn("exceeds", manifest["verification"]["decision"])
        self.assertNotIn("saturated_repetitions", manifest["verification"])

    def test_local_ydb_verification_rejects_an_empty_repetition_when_errors_are_allowed(self):
        configuration = load_config(self._config("""
            local-ydb:
              empty-holdout:
                workload: {type: kv, operation: upsert}
                load: {parameter: threads, allow-errors: true, values: [1]}
                measurement: {warmup: 0, duration: 1, repetitions: 1, verification-repetitions: 2}
                affinity:
                  ydb-cli: {mode: none}
                  static-nodes: {mode: none}
                  dynamic-nodes: {mode: none}
        """)).runs[0]
        manifest, _output, _events = self._run_mock_local_ydb(
            configuration,
            "verification-empty-repetition",
            [
                {"transactions": 10, "throughput": 10, "errors": 1},
                {"transactions": 10, "throughput": 9, "errors": 1},
                {"transactions": 0, "throughput": 0, "errors": 10},
            ],
        )
        self.assertEqual(manifest["state"], "passed")
        self.assertEqual(manifest["result"]["selected_load"], 1)
        self.assertEqual(manifest["result"]["verified_metrics"]["empty_repetitions"], 1)
        self.assertFalse(manifest["result"]["holdout_accepted"])
        self.assertFalse(manifest["verification"]["accepted"])
        self.assertIn("zero successful operations", manifest["verification"]["decision"])

    def test_local_ydb_verification_failure_preserves_search_and_partial_holdout(self):
        configuration = load_config(self._config("""
            local-ydb:
              verification-failure:
                workload: {type: kv, operation: upsert}
                load: {parameter: threads, values: [1]}
                measurement: {warmup: 0, duration: 1, repetitions: 1, verification-repetitions: 2}
                affinity:
                  ydb-cli: {mode: none}
                  static-nodes: {mode: none}
                  dynamic-nodes: {mode: none}
        """)).runs[0]
        output = self.root / "verification-failure"
        with self.assertRaisesRegex(BenchmarkError, "exited with code 17"):
            self._run_mock_local_ydb(
                configuration,
                output.name,
                [
                    {"throughput": 10},
                    {"throughput": 20},
                    {"throughput": 0, "exit_code": 17, "stderr": "verification command failed"},
                ],
            )
        manifest = json.loads((output / "run.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["state"], "failed")
        self.assertEqual(manifest["verification"]["status"], "failed")
        self.assertEqual(manifest["verification"]["completed_repetitions"], 1)
        self.assertEqual(manifest["result"]["selected_metrics"]["throughput"], 10)
        self.assertEqual(manifest["result"]["metrics_source"], "search")
        self.assertTrue((output / "repetitions.csv").is_file())
        with (output / "verification-repetitions.csv").open(newline="", encoding="utf-8") as stream:
            self.assertEqual(len(list(csv.DictReader(stream))), 1)
        self.last_local_ydb_cluster.stop.assert_called_once_with()

    def test_local_ydb_verification_cancellation_is_durable(self):
        configuration = load_config(self._config("""
            local-ydb:
              verification-cancel:
                workload: {type: kv, operation: upsert}
                load: {parameter: threads, values: [1]}
                measurement: {warmup: 0, duration: 1, repetitions: 1, verification-repetitions: 2}
                affinity:
                  ydb-cli: {mode: none}
                  static-nodes: {mode: none}
                  dynamic-nodes: {mode: none}
        """)).runs[0]
        output = self.root / "verification-cancel"
        with self.assertRaisesRegex(BenchmarkInterrupted, "workload was interrupted"):
            self._run_mock_local_ydb(
                configuration,
                output.name,
                [{"throughput": 10}, {"throughput": 0, "interrupted": True}],
            )
        manifest = json.loads((output / "run.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["state"], "cancelled")
        self.assertEqual(manifest["verification"]["status"], "cancelled")
        self.assertEqual(manifest["verification"]["completed_repetitions"], 0)
        self.assertEqual(manifest["result"]["selected_metrics"]["throughput"], 10)
        self.assertNotIn("repetitions_file", manifest["verification"])
        self.assertNotIn("summary_file", manifest["verification"])
        self.assertTrue((output / "summary.csv").is_file())
        self.assertTrue((output / "verification" / "repeat-001" / "stdout.txt").is_file())
        self.last_local_ydb_cluster.stop.assert_called_once_with()

    def test_local_ydb_kv_commands_keep_table_path(self):
        cli_context = local_ydb_workloads.WorkloadCli(
            Path("ydb"),
            "grpc://host.example:2135",
            "/Root/bench",
        )
        workload = local_ydb_workloads.normalize_workload(
            {"type": "kv", "operation": "upsert"},
            "workload",
        )
        command = local_ydb_workloads.build_init_argv(cli_context, "table-prefix", workload)
        self.assertEqual(command[command.index("--path") : command.index("--path") + 2], ["--path", "table-prefix"])
        self.assertEqual(local_ydb_workloads.workload_table_path("kv", "table-prefix"), "table-prefix")

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

    def test_load_controllers_reject_automatic_search_that_exceeds_attempt_budget(self):
        measure = mock.Mock()
        latency = {
            "parameter": "rate",
            "search": {
                "start": 1,
                "maximum": 1000000,
                "multiplier": 1.000001,
                "resolution_percent": 2,
            },
            "objective": {
                "type": "latency-slo",
                "percentile": "p99",
                "max_ms": 10,
                "max_errors": 0,
                "min_achieved_rate_ratio": 0.98,
            },
        }
        with self.assertRaisesRegex(BenchmarkError, "more than 64 attempts"):
            load_control.search_load(latency, measure)
        measure.assert_not_called()

        throughput = {
            "parameter": "rate",
            "search": {
                "start": 1,
                "maximum": 1000000,
                "multiplier": 2,
                "resolution_percent": 0.000001,
            },
            "objective": {
                "type": "maximize-throughput",
                "target_role": "dynamic",
                "cpu_saturation_percent": 90,
                "plateau_gain_percent": 1,
                "plateau_points": 2,
            },
        }
        with self.assertRaisesRegex(BenchmarkError, "more than 64 attempts"):
            load_control.search_load(throughput, measure)
        measure.assert_not_called()

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

    def test_evaluate_load_reuses_search_acceptance_for_verification(self):
        points = {"parameter": "threads", "values": [1]}
        self.assertEqual(load_control.evaluate_load(points, 1, {"errors": 0}), (True, "configured point"))
        self.assertFalse(load_control.evaluate_load(points, 1, {"errors": 1})[0])
        self.assertTrue(load_control.evaluate_load({**points, "allow_errors": True}, 1, {"errors": 1})[0])

        latency = {
            "parameter": "rate",
            "objective": {
                "type": "latency-slo",
                "percentile": "p99",
                "max_ms": 10,
                "max_errors": 0,
                "min_achieved_rate_ratio": 0.98,
            },
        }
        self.assertTrue(load_control.evaluate_load(latency, 100, {"throughput": 100, "errors": 0, "p99_ms": 9})[0])
        passed, reason = load_control.evaluate_load(
            latency,
            100,
            {"throughput": 100, "errors": 0, "p99_ms": 11},
        )
        self.assertFalse(passed)
        self.assertIn("exceeds", reason)

    def test_load_controllers_reject_zero_success_measurements_before_objectives(self):
        point_configs = (
            {"parameter": "threads", "values": [1]},
            {"parameter": "threads", "allow_errors": True, "values": [1]},
        )
        for config in point_configs:
            with self.subTest(controller="points", allow_errors=config.get("allow_errors", False)):
                result = load_control.search_load(
                    config,
                    lambda _load: {"transactions": 0, "throughput": 0, "errors": 10},
                )
                self.assertIsNone(result.selected_load)
                self.assertFalse(result.attempts[0]["passed"])
                self.assertIn("zero successful operations", result.attempts[0]["decision"])

        throughput = load_control.search_load(
            {
                "parameter": "threads",
                "allow_errors": True,
                "search": {"start": 10, "maximum": 100, "multiplier": 2, "resolution_percent": 2},
                "objective": {
                    "type": "maximize-throughput",
                    "target_role": "dynamic",
                    "cpu_saturation_percent": 90,
                    "plateau_gain_percent": 1,
                    "plateau_points": 2,
                },
            },
            lambda load: {
                "transactions": 0 if load >= 70 else 1,
                "throughput": 100 - abs(load - 55),
                "errors": 0,
                "dynamic_cpu_mean": 50,
                "static_cpu_mean": 10,
                "host_cpu_mean": 20,
            },
        )
        self.assertEqual(throughput.outcome, "bounded-by-invalid-sample")
        self.assertIn("invalid measurement", throughput.stop_reason)

        latency = load_control.search_load(
            {
                "parameter": "threads",
                "allow_errors": True,
                "search": {"start": 10, "maximum": 40, "multiplier": 2, "resolution_percent": 5},
                "objective": {
                    "type": "latency-slo",
                    "percentile": "p99",
                    "max_ms": 10,
                    "max_errors": 0,
                    "min_achieved_rate_ratio": 0.98,
                },
            },
            lambda load: {
                "transactions": 0 if load >= 20 else 1,
                "throughput": load,
                "errors": 0,
                "p99_ms": 1,
            },
        )
        self.assertEqual(latency.selected_load, 19)
        self.assertEqual(latency.outcome, "bounded-by-invalid-sample")
        self.assertIn("invalid measurement", latency.stop_reason)

        for allow_errors in (False, True):
            with self.subTest(controller="maximize-throughput", allow_errors=allow_errors):
                result = load_control.search_load(
                    {
                        "parameter": "threads",
                        "allow_errors": allow_errors,
                        "search": {"start": 1, "maximum": 2, "multiplier": 2, "resolution_percent": 5},
                        "objective": {
                            "type": "maximize-throughput",
                            "target_role": "dynamic",
                            "cpu_saturation_percent": 90,
                            "plateau_gain_percent": 1,
                            "plateau_points": 2,
                        },
                    },
                    lambda _load: {
                        "transactions": 0,
                        "throughput": 0,
                        "errors": 10,
                        "dynamic_cpu_mean": 100,
                        "static_cpu_mean": 10,
                        "host_cpu_mean": 50,
                    },
                )
                self.assertIsNone(result.selected_load)
                self.assertFalse(result.attempts[0]["passed"])
                self.assertIn("zero successful operations", result.attempts[0]["decision"])

            with self.subTest(controller="latency-slo", allow_errors=allow_errors):
                result = load_control.search_load(
                    {
                        "parameter": "threads",
                        "allow_errors": allow_errors,
                        "search": {"start": 1, "maximum": 2, "multiplier": 2, "resolution_percent": 5},
                        "objective": {
                            "type": "latency-slo",
                            "percentile": "p99",
                            "max_ms": 10,
                            "max_errors": 0,
                            "min_achieved_rate_ratio": 0.98,
                        },
                    },
                    lambda _load: {"transactions": 0, "throughput": 0, "errors": 10, "p99_ms": 1},
                )
                self.assertIsNone(result.selected_load)
                self.assertFalse(result.attempts[0]["passed"])
                self.assertIn("zero successful operations", result.attempts[0]["decision"])

    def test_evaluate_load_remains_compatible_without_success_metadata(self):
        config = {"parameter": "threads", "allow_errors": True, "values": [1]}
        self.assertTrue(load_control.evaluate_load(config, 1, {"throughput": 0, "errors": 1})[0])

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

    def test_linux_cpu_summary_filters_complete_intervals_inside_measurement_window(self):
        monitor = linux_telemetry.LinuxCpuMonitor({"dynamic": lambda: ()}, {"dynamic": 1}, interval=0.5)
        monitor._records = [
            {"elapsed_seconds": 1.0, "timestamp_unix": 101.0, "dynamic_cpu": 10.0, "host_cpu": 15.0},
            {"elapsed_seconds": 1.0, "timestamp_unix": 102.0, "dynamic_cpu": 20.0, "host_cpu": 25.0},
            {"elapsed_seconds": 1.0, "timestamp_unix": 103.0, "dynamic_cpu": 30.0, "host_cpu": 35.0},
            {"elapsed_seconds": 1.0, "timestamp_unix": 104.0, "dynamic_cpu": 40.0, "host_cpu": 45.0},
        ]
        summary = monitor.summary(started_at_unix=101.0, finished_at_unix=103.0)
        self.assertEqual(summary["dynamic_cpu_mean"], 25.0)
        self.assertEqual(summary["dynamic_cpu_max"], 30.0)
        self.assertEqual(summary["host_cpu_mean"], 30.0)
        self.assertEqual(summary["host_cpu_max"], 35.0)

    def test_linux_cpu_summary_rejects_invalid_or_empty_measurement_window(self):
        monitor = linux_telemetry.LinuxCpuMonitor({"dynamic": lambda: ()}, {"dynamic": 1})
        monitor._records = [
            {"elapsed_seconds": 1.0, "timestamp_unix": 101.0, "dynamic_cpu": 10.0, "host_cpu": 15.0},
        ]
        for start, finish in (
            (100.0, None),
            (101.0, 101.0),
            (float("nan"), 102.0),
            (True, 102.0),
            (10**1000, 10**1000 + 1),
        ):
            with self.subTest(start=start, finish=finish), self.assertRaisesRegex(BenchmarkError, "CPU measurement"):
                monitor.summary(started_at_unix=start, finished_at_unix=finish)
        with self.assertRaisesRegex(BenchmarkError, "does not contain"):
            monitor.summary(started_at_unix=102.0, finished_at_unix=103.0)

        default_summary = monitor.summary()
        self.assertEqual(default_summary["dynamic_cpu_mean"], 10.0)
        self.assertEqual(default_summary["host_cpu_mean"], 15.0)

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

    def test_local_ydb_attempt_rejects_one_empty_repetition(self):
        metrics = local_ydb._aggregate_measurements(
            [
                {"transactions": 10, "throughput": 10, "errors": 1},
                {"transactions": 0, "throughput": 0, "errors": 10},
                {"transactions": 20, "throughput": 20, "errors": 2},
            ]
        )
        passed, reason = load_control.evaluate_load(
            {"parameter": "threads", "allow_errors": True, "values": [1]},
            1,
            metrics,
        )
        self.assertEqual(metrics["empty_repetitions"], 1)
        self.assertEqual(metrics["errors"], 13)
        self.assertFalse(passed)
        self.assertIn("1 repetition", reason)
        self.assertIn("zero successful operations", reason)

    def test_local_ydb_attempt_allows_partial_errors_when_every_repetition_succeeds(self):
        metrics = local_ydb._aggregate_measurements(
            [
                {"transactions": 10, "throughput": 10, "errors": 1},
                {"transactions": 20, "throughput": 20, "errors": 2},
            ]
        )
        passed, reason = load_control.evaluate_load(
            {"parameter": "threads", "allow_errors": True, "values": [1]},
            1,
            metrics,
        )
        self.assertEqual(metrics["empty_repetitions"], 0)
        self.assertEqual(metrics["errors"], 3)
        self.assertTrue(passed)
        self.assertIn("errors allowed", reason)

    def test_local_ydb_summary_omits_points_with_an_empty_repetition(self):
        rows = [
            {"load": 1, "dynamic_nodes": 1, "transactions": 10},
            {"load": 1, "dynamic_nodes": 1, "transactions": 0},
            {"load": 2, "dynamic_nodes": 1, "transactions": 20},
        ]
        for row in rows:
            row.update({metric.name: row.get(metric.name, 1) for metric in LOCAL_YDB_BENCHMARK.metrics})
        summary = LOCAL_YDB_BENCHMARK.summarize_metrics(rows, LOCAL_YDB_BENCHMARK)
        self.assertEqual([(row["load"], row["dynamic_nodes"]) for row in summary], [(2, 1)])

    def test_local_ydb_summary_exports_schema_aggregation(self):
        rows = [
            {"load": 10, "dynamic_nodes": 1, "throughput": 100, "errors": 1},
            {"load": 10, "dynamic_nodes": 1, "throughput": 120, "errors": 1},
            {"load": 10, "dynamic_nodes": 1, "throughput": 110, "errors": 1},
        ]
        metric_names = ("throughput", "errors")
        aggregations = {"throughput": "median", "errors": "sum"}
        summary = LOCAL_YDB_BENCHMARK.summarize_metrics(
            rows,
            LOCAL_YDB_BENCHMARK,
            metric_names,
            aggregations,
        )
        self.assertEqual(summary[0]["median_throughput"], 110)
        self.assertEqual(summary[0]["max_errors"], 1)
        self.assertEqual(summary[0]["sum_errors"], 3)
        rendered = LOCAL_YDB_BENCHMARK.render_summary(
            summary,
            LOCAL_YDB_BENCHMARK,
            metric_names,
            aggregations,
        )
        parsed = next(csv.DictReader(io.StringIO(rendered)))
        self.assertEqual(parsed["sum_errors"], "3")

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
  Allocated pools:
    ssd: 1/1
  Allocated units:
  Registered units:
  Data size hard quota: 0
  Data size soft quota: 0
"""
        self.assertTrue(local_ydb._database_status_ready(status))
        self.assertFalse(local_ydb._database_status_ready(status.replace("RUNNING", "PENDING_RESOURCES")))

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

    def test_local_ydb_scaling_waits_for_database_and_every_new_node(self):
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
        ) as wait_tenant, mock.patch.object(
            local_ydb, "start_managed_process", side_effect=(mock.Mock(pid=101), mock.Mock(pid=102))
        ):
            cluster.add_dynamic_nodes(2)

        self.assertEqual(
            wait_for_port.call_args_list,
            [mock.call(2136, "dynamic node 1"), mock.call(2137, "dynamic node 2")],
        )
        wait_database.assert_called_once_with()
        wait_tenant.assert_called_once_with(30)

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

    def _local_ydb_result(
        self,
        directory,
        throughput,
        operation="put",
        verified=False,
        result_schema=None,
        extra_metrics=None,
    ):
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
        selected_metrics.update(extra_metrics or {})
        result = {
            "outcome": "best-observed",
            "parameter": "rate",
            "dynamic_nodes": 1,
            "selected_load": 100,
            "selected_metrics": selected_metrics,
        }
        verification = None
        if verified:
            result.update(
                {
                    "metrics_source": "verification",
                    "holdout_accepted": True,
                    "verified_metrics": {
                        **selected_metrics,
                        "throughput": throughput + 50,
                        "commands": [{"argv": ["must", "also", "not", "leak"]}],
                    },
                }
            )
            verification = {
                "status": "completed",
                "configured_repetitions": 3,
                "completed_repetitions": 3,
                "accepted": True,
                "evaluation_kind": "validity",
                "decision": "workload completed without errors",
                "throughput_delta_percent": 5,
                "saturated_repetitions": 3,
                "samples": [{"commands": [{"argv": ["not", "in", "comparison"]}]}],
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
            "result": result,
        }
        if result_schema is not None:
            profile["workload_result_schema"] = result_schema
        if verification is not None:
            profile["verification"] = verification
        (profile_root / "run.json").write_text(json.dumps(profile), encoding="utf-8")

    def _custom_local_ydb_result_schema(self, schema_id="fake-json-v1", queue_unit="messages"):
        return {
            "schema_id": schema_id,
            "metrics": [
                {
                    "name": "throughput",
                    "unit": "widgets/s",
                    "repetition_aggregation": "median",
                    "required": True,
                    "description": "delivered widgets",
                },
                {
                    "name": "latency_ms",
                    "unit": "ms",
                    "repetition_aggregation": "median",
                    "required": True,
                    "description": "fake SLO latency",
                },
                {
                    "name": "queue_depth",
                    "unit": queue_unit,
                    "repetition_aggregation": "median",
                    "required": False,
                    "description": "queued widgets",
                },
            ],
            "slo_metrics": {"p90": "latency_ms"},
            "throughput_unit": "widgets/s",
            "reports_errors": False,
        }

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

    def test_run_service_compacts_oversized_event_log_records_without_changing_lifecycle(self):
        def oversized_executor(run, emit, _cancelled):
            step_id = run["store"].manifest["steps"][0]["id"]
            emit({"type": "step-started", "step_id": step_id})
            emit(
                {
                    "type": "step-finished",
                    "step_id": step_id,
                    "state": "passed",
                    "fields": {"reason": "x" * 4096},
                }
            )

        with mock.patch.object(web, "_EVENT_LOG_RECORD_BYTES", 512):
            service = RunService(self.root, executor=oversized_executor)
            try:
                run_id = service.start(
                    "ping-bench:\n  oversized: {threads: [1], duration: 1, repetitions: 1, affinity: [none]}\n"
                )["id"]
                run = service._runs[run_id]
                self.assertTrue(run["finished"].wait(2))
                manifest = json.loads((run["root"] / "run.json").read_text(encoding="utf-8"))
                self.assertEqual(manifest["state"], "passed")
                self.assertEqual(manifest["steps"][0]["state"], "passed")
                persisted = [
                    json.loads(line) for line in (run["root"] / "events.jsonl").read_text(encoding="utf-8").splitlines()
                ]
                compacted = next(event for event in persisted if event.get("payload_truncated"))
                self.assertEqual(compacted["type"], "step-finished")
                self.assertEqual(compacted["state"], "passed")
                self.assertGreater(compacted["original_size_bytes"], web._EVENT_LOG_RECORD_BYTES)
                self.assertTrue(
                    all(
                        len((json.dumps(event, sort_keys=True) + "\n").encode("utf-8")) <= web._EVENT_LOG_RECORD_BYTES
                        for event in persisted
                    )
                )
            finally:
                service.shutdown()

    def test_run_service_keeps_original_failure_when_compacting_oversized_event(self):
        def oversized_executor(run, emit, _cancelled):
            step_id = run["store"].manifest["steps"][0]["id"]
            emit({"type": "step-started", "step_id": step_id})
            emit(
                {
                    "type": "step-finished",
                    "step_id": step_id,
                    "state": "failed",
                    "fields": {"error": "x" * 4096},
                }
            )
            raise BenchmarkError("original benchmark failure")

        with mock.patch.object(web, "_EVENT_LOG_RECORD_BYTES", 512):
            service = RunService(self.root, executor=oversized_executor)
            try:
                run_id = service.start(
                    "ping-bench:\n  oversized: {threads: [1], duration: 1, repetitions: 1, affinity: [none]}\n"
                )["id"]
                run = service._runs[run_id]
                self.assertTrue(run["finished"].wait(2))
                manifest = json.loads((run["root"] / "run.json").read_text(encoding="utf-8"))
                self.assertEqual(manifest["state"], "failed")
                self.assertEqual(manifest["steps"][0]["state"], "failed")
                self.assertEqual(manifest["error"], "original benchmark failure")
                persisted = [
                    json.loads(line) for line in (run["root"] / "events.jsonl").read_text(encoding="utf-8").splitlines()
                ]
                compacted = next(event for event in persisted if event.get("payload_truncated"))
                self.assertEqual(compacted["type"], "step-finished")
                self.assertEqual(compacted["state"], "failed")
            finally:
                service.shutdown()

    def test_local_ydb_activity_is_profile_scoped_bounded_and_strictly_projected(self):
        run_root = self.root / "local-ydb-activity"
        self._manifest(run_root)
        manifest_path = run_root / "run.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["runs"] = [
            {"benchmark": "local-ydb", "profile": "capacity", "status": "running"},
            {"benchmark": "local-ydb", "profile": "other", "status": "running"},
        ]
        manifest["steps"] = [
            {
                "id": "capacity-step",
                "benchmark": "local-ydb",
                "profile": "capacity",
                "state": "running",
            },
            {
                "id": "other-step",
                "benchmark": "local-ydb",
                "profile": "other",
                "state": "running",
            },
        ]
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        events = [
            {
                "sequence": 1,
                "at": "2025-01-01T00:00:00+00:00",
                "type": "step-started",
                "step_id": "capacity-step",
                "fields": {"role_affinity": {"private": list(range(1000))}},
            }
        ]
        for index in range(205):
            progress = {
                "phase": "measuring",
                "search_stage": 1,
                "attempt": index + 1,
                "parameter": "rate",
                "load": 1000 + index,
                "latest_attempt": {"commands": [{"private": "not projected"}]},
            }
            if index == 204:
                progress.update(
                    {
                        "current_command": {
                            "phase": "measuring",
                            "repetition": 2,
                            "argv": ["/tmp/ydb", "workload", "run"],
                            "cpu_affinity": [0, 128],
                            "private": "not projected",
                        },
                        "verification": {
                            "status": "completed",
                            "configured_repetitions": 3,
                            "completed_repetitions": 3,
                            "accepted": True,
                            "evaluation_kind": "objective",
                            "decision": "latency-slo-passed",
                            "samples": [{"commands": ["not projected"]}],
                        },
                    }
                )
            events.append(
                {
                    "sequence": index + 2,
                    "at": "2025-01-01T00:00:{:02d}+00:00".format(index % 60),
                    "type": "step-progress",
                    "step_id": "capacity-step",
                    "fields": {"progress": progress},
                }
            )
        last_target_sequence = events[-1]["sequence"]
        events.append(
            {
                "sequence": last_target_sequence + 1,
                "at": "2025-01-01T00:04:00+00:00",
                "type": "step-progress",
                "step_id": "other-step",
                "fields": {"progress": {"phase": "measuring", "attempt": 999}},
            }
        )
        (run_root / "events.jsonl").write_text(
            "".join(json.dumps(event) + "\n" for event in events),
            encoding="utf-8",
        )

        service = RunService(self.root)
        try:
            with mock.patch.object(service, "events", side_effect=AssertionError("must stream event log")):
                activity = service.local_ydb_activity("local-ydb-activity", "capacity")
            self.assertTrue(activity["truncated"])
            self.assertEqual(len(activity["events"]), web._LOCAL_YDB_ACTIVITY_LIMIT)
            self.assertEqual(activity["after"], last_target_sequence + 1)
            self.assertTrue(all(item["sequence"] <= last_target_sequence for item in activity["events"]))
            projected = activity["events"][-1]
            self.assertEqual(projected["current_command"]["argv"], ["/tmp/ydb", "workload", "run"])
            self.assertEqual(projected["current_command"]["cpu_affinity"], [0, 128])
            self.assertNotIn("private", projected["current_command"])
            self.assertNotIn("latest_attempt", projected)
            self.assertTrue(projected["verification"]["accepted"])
            self.assertEqual(projected["verification"]["evaluation_kind"], "objective")
            self.assertNotIn("samples", projected["verification"])
            replay = service.local_ydb_activity("local-ydb-activity", "capacity", after=last_target_sequence)
            self.assertEqual(replay, {"events": [], "after": last_target_sequence + 1, "truncated": False})
            live_event = {
                "sequence": last_target_sequence + 2,
                "type": "step-progress",
                "step_id": "capacity-step",
                "fields": {"progress": {"phase": "finishing"}},
            }
            service._runs["local-ydb-activity"] = {
                "lock": threading.RLock(),
                "events": deque([live_event]),
                "store": mock.Mock(manifest={"events": live_event["sequence"]}),
            }
            try:
                with mock.patch.object(web, "load_manifest", return_value=manifest), mock.patch.object(
                    Path, "open", side_effect=AssertionError("live poll must not replay the event log")
                ):
                    live = service.local_ydb_activity("local-ydb-activity", "capacity", after=last_target_sequence + 1)
                self.assertEqual(live["after"], live_event["sequence"])
                self.assertEqual(live["events"][0]["phase"], "finishing")
            finally:
                del service._runs["local-ydb-activity"]
            with self.assertRaisesRegex(BenchmarkError, "non-negative integer"):
                service.local_ydb_activity("local-ydb-activity", "capacity", after=-1)
            with self.assertRaisesRegex(BenchmarkError, "JSON safe range"):
                service.local_ydb_activity("local-ydb-activity", "capacity", after=web._MAX_SAFE_JSON_INTEGER + 1)
            with self.assertRaisesRegex(BenchmarkError, "profile not found"):
                service.local_ydb_activity("local-ydb-activity", "missing")
            (run_root / "events.jsonl").write_text(
                json.dumps({"sequence": "one", "type": "step-progress"}) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(BenchmarkError, "strictly increasing integers"):
                service.local_ydb_activity("local-ydb-activity", "capacity")
            (run_root / "events.jsonl").write_text(
                json.dumps({"sequence": 1, "type": 7}) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(BenchmarkError, "type must be a string"):
                service.local_ydb_activity("local-ydb-activity", "capacity")
            (run_root / "events.jsonl").write_text(
                json.dumps({"sequence": web._MAX_SAFE_JSON_INTEGER + 1, "type": "step-progress"}) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(BenchmarkError, "strictly increasing integers"):
                service.local_ydb_activity("local-ydb-activity", "capacity")

            large_events = []
            for sequence in range(1, 41):
                large_events.append(
                    {
                        "sequence": sequence,
                        "type": "step-progress",
                        "step_id": "capacity-step",
                        "fields": {
                            "progress": {
                                "phase": "measuring",
                                "current_command": {"argv": ["x" * 512] * 64},
                            }
                        },
                    }
                )
            (run_root / "events.jsonl").write_text(
                "".join(json.dumps(event) + "\n" for event in large_events),
                encoding="utf-8",
            )
            bounded = service.local_ydb_activity("local-ydb-activity", "capacity")
            self.assertTrue(bounded["truncated"])
            self.assertLess(len(bounded["events"]), len(large_events))
            self.assertLessEqual(
                len(json.dumps(bounded["events"], separators=(",", ":")).encode("utf-8")),
                web._LOCAL_YDB_ACTIVITY_RESPONSE_BYTES + 2,
            )
        finally:
            service.shutdown()

    def test_local_ydb_activity_http_endpoint_validates_profile_and_cursor(self):
        run_root = self.root / "local-ydb-http-activity"
        self._manifest(run_root)
        manifest_path = run_root / "run.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["runs"] = [{"benchmark": "local-ydb", "profile": "capacity", "status": "running"}]
        manifest["steps"] = [
            {
                "id": "capacity-step",
                "benchmark": "local-ydb",
                "profile": "capacity",
                "state": "running",
            }
        ]
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        (run_root / "events.jsonl").write_text(
            json.dumps(
                {
                    "sequence": 1,
                    "at": "2025-01-01T00:00:00+00:00",
                    "type": "step-progress",
                    "step_id": "capacity-step",
                    "fields": {"progress": {"phase": "warming-up", "load": 1000}},
                }
            )
            + "\n",
            encoding="utf-8",
        )
        server = make_server("127.0.0.1", 0, self.root)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        base = "http://127.0.0.1:{}".format(server.server_port)
        endpoint = base + "/api/runs/local-ydb-http-activity/local-ydb-activity"
        try:
            with urllib.request.urlopen(endpoint + "?profile=capacity&after=0") as response:
                activity = json.loads(response.read())
            self.assertEqual(activity["after"], 1)
            self.assertEqual(activity["events"][0]["phase"], "warming-up")
            for query, message in (
                ("?after=0", "profile is required"),
                ("?profile=capacity&after=abc", "non-negative integer"),
                ("?profile=capacity&after=-1", "non-negative integer"),
                (
                    "?profile=capacity&after={}".format(web._MAX_SAFE_JSON_INTEGER + 1),
                    "JSON safe range",
                ),
            ):
                with self.assertRaises(HTTPError) as context:
                    urllib.request.urlopen(endpoint + query)
                self.assertEqual(context.exception.code, 400)
                self.assertIn(message, json.loads(context.exception.read())["error"])
        finally:
            server.shutdown()
            worker.join()
            server.server_close()

    def test_local_ydb_activity_bounds_persisted_replay_to_recent_tail(self):
        run_root = self.root / "local-ydb-tail-activity"
        self._manifest(run_root)
        manifest_path = run_root / "run.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["runs"] = [{"benchmark": "local-ydb", "profile": "capacity", "status": "running"}]
        manifest["steps"] = [
            {
                "id": "capacity-step",
                "benchmark": "local-ydb",
                "profile": "capacity",
                "state": "running",
            }
        ]
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        events = [
            {
                "sequence": sequence,
                "type": "step-progress",
                "step_id": "capacity-step",
                "fields": {"progress": {"phase": "measuring", "attempt": sequence, "padding": "x" * 80}},
            }
            for sequence in range(1, 21)
        ]
        (run_root / "events.jsonl").write_text(
            "".join(json.dumps(event, separators=(",", ":")) + "\n" for event in events),
            encoding="utf-8",
        )

        with mock.patch.object(web, "_LOCAL_YDB_ACTIVITY_SCAN_BYTES", 256), mock.patch.object(
            web, "_EVENT_LOG_RECORD_BYTES", 512
        ):
            service = RunService(self.root)
            try:
                activity = service.local_ydb_activity("local-ydb-tail-activity", "capacity")
                self.assertTrue(activity["truncated"])
                self.assertGreater(activity["events"][0]["sequence"], 1)
                self.assertEqual(activity["events"][-1]["sequence"], 20)
                self.assertEqual(activity["after"], 20)

                first_sequence = activity["events"][0]["sequence"]
                replay = service.local_ydb_activity(
                    "local-ydb-tail-activity",
                    "capacity",
                    after=first_sequence - 1,
                )
                self.assertFalse(replay["truncated"])
                self.assertEqual(replay["events"], activity["events"])
                self.assertEqual(replay["after"], 20)
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
        self._local_ydb_result(self.root / "baseline", 1000, verified=True)
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
            self.assertEqual(comparison["entries"][0]["result"]["metrics_source"], "verification")
            self.assertEqual(comparison["entries"][0]["result"]["verified_metrics"]["throughput"], 1050)
            self.assertNotIn("commands", comparison["entries"][0]["result"]["verified_metrics"])
            self.assertTrue(comparison["entries"][0]["verification"]["accepted"])
            self.assertNotIn("samples", comparison["entries"][0]["verification"])
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

    def test_local_ydb_profile_and_comparison_project_custom_result_schema(self):
        schema = self._custom_local_ydb_result_schema()
        self._local_ydb_result(
            self.root / "custom-schema",
            123,
            verified=True,
            result_schema=schema,
            extra_metrics={"latency_ms": 4.5, "queue_depth": 17},
        )
        service = RunService(self.root)
        try:
            profile = service.local_ydb_profile("custom-schema", "capacity")
            comparison = service.local_ydb_comparison(["custom-schema"])["entries"][0]
        finally:
            service.shutdown()

        self.assertEqual(profile["workload_result_schema"], schema)
        self.assertEqual(comparison["workload_result_schema"], schema)
        self.assertEqual(comparison["result"]["selected_metrics"]["latency_ms"], 4.5)
        self.assertEqual(comparison["result"]["selected_metrics"]["queue_depth"], 17)
        self.assertEqual(comparison["result"]["verified_metrics"]["queue_depth"], 17)
        self.assertNotIn("errors", comparison["result"]["selected_metrics"])
        self.assertNotIn("commands", comparison["result"]["selected_metrics"])

    def test_local_ydb_profile_uses_historical_schema_when_manifest_has_none(self):
        self._local_ydb_result(self.root / "legacy-schema", 100)
        service = RunService(self.root)
        try:
            schema = service.local_ydb_profile("legacy-schema", "capacity")["workload_result_schema"]
        finally:
            service.shutdown()

        self.assertEqual(schema["schema_id"], "generic-total-v1")
        self.assertEqual(schema["throughput_unit"], "requests/s")
        self.assertEqual(schema["slo_metrics"]["p99"], "p99_ms")
        self.assertTrue(schema["reports_errors"])

    def test_local_ydb_result_schema_projection_rejects_incompatible_contracts(self):
        schema = self._custom_local_ydb_result_schema()
        invalid = []
        invalid.append({**schema, "schema_id": "Unsafe schema"})
        invalid.append(
            {
                **schema,
                "metrics": [
                    {**schema["metrics"][0], "name": "static_cpu_mean"},
                    *schema["metrics"][1:],
                ],
            }
        )
        invalid.append({**schema, "throughput_unit": "requests/s"})
        invalid.append({**schema, "reports_errors": True})
        invalid.append({**schema, "slo_metrics": {"latency": "latency_ms"}})
        invalid.append(
            {
                **schema,
                "metrics": [
                    schema["metrics"][0],
                    {**schema["metrics"][1], "unit": "seconds"},
                    schema["metrics"][2],
                ],
            }
        )
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(BenchmarkError):
                web._project_local_ydb_result_schema(value)
        with self.assertRaisesRegex(BenchmarkError, "schema must be an object"):
            web._resolved_local_ydb_result_schema(
                {"parameters": {"workload": {"type": "kv"}}, "workload_result_schema": None}
            )

    def test_local_ydb_comparison_projects_empty_holdout_repetitions(self):
        run_root = self.root / "empty-holdout"
        self._local_ydb_result(run_root, 1000, verified=True)
        profile_path = run_root / "local-ydb" / "capacity" / "run.json"
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
        profile["result"]["holdout_accepted"] = False
        profile["result"]["verified_metrics"].update(
            {"empty_repetitions": 1, "p99_ms": 0, "throughput": 0, "errors": 10}
        )
        profile["verification"].update(
            {"accepted": False, "decision": "invalid measurement: zero successful operations"}
        )
        profile_path.write_text(json.dumps(profile), encoding="utf-8")

        service = RunService(self.root)
        try:
            comparison = service.local_ydb_comparison(["empty-holdout"])
        finally:
            service.shutdown()
        result = comparison["entries"][0]["result"]
        self.assertEqual(result["verified_metrics"]["empty_repetitions"], 1)
        self.assertEqual(result["verified_metrics"]["p99_ms"], 0)
        self.assertFalse(result["holdout_accepted"])

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

    def test_chart_data_bounds_total_rows_across_selected_runs(self):
        for run_id in ("first", "second"):
            self._manifest(self.root / run_id)
            summary = self.root / run_id / "ping-bench" / "baseline" / "summary.csv"
            summary.parent.mkdir(parents=True)
            summary.write_text(
                "affinity_mode,threads,median_msgs_per_sec\n" "none,1,10\n" "none,2,20\n",
                encoding="utf-8",
            )

        with mock.patch.object(web, "_CHART_DATA_ROW_LIMIT", 3), self.assertRaisesRegex(
            BenchmarkError, "selected chart data has too many rows"
        ):
            chart_data(self.root, ["first", "second"])

    def test_local_ydb_summary_is_available_to_comparison_charts(self):
        self._manifest(self.root / "complete")
        summary = self.root / "complete" / "local-ydb" / "capacity" / "summary.csv"
        summary.parent.mkdir(parents=True)
        rows = []
        for load, dynamic_nodes, scale in ((10, 1, 1), (20, 1, 2), (10, 2, 3)):
            row = {"load": load, "dynamic_nodes": dynamic_nodes}
            row.update({metric.name: (index + 1) * scale for index, metric in enumerate(LOCAL_YDB_BENCHMARK.metrics)})
            rows.append(row)
        aggregations = {"errors": "sum"}
        summarized = LOCAL_YDB_BENCHMARK.summarize_metrics(
            rows,
            LOCAL_YDB_BENCHMARK,
            metric_aggregations=aggregations,
        )
        summary.write_text(
            LOCAL_YDB_BENCHMARK.render_summary(
                summarized,
                LOCAL_YDB_BENCHMARK,
                metric_aggregations=aggregations,
            ),
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
        self.assertIn("sum_errors", value["metrics"])
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

    def test_local_ydb_chart_data_uses_persisted_custom_metric_metadata(self):
        for run_id, queue_unit in (("custom-one", "messages"), ("custom-two", "bytes")):
            self._manifest(self.root / run_id)
            profile_root = self.root / run_id / "local-ydb" / "capacity"
            profile_root.mkdir(parents=True)
            profile_root.joinpath("summary.csv").write_text(
                "affinity_mode,load,dynamic_nodes,samples,median_throughput,min_throughput,max_throughput,"
                "median_latency_ms,min_latency_ms,max_latency_ms,median_queue_depth,min_queue_depth,max_queue_depth\n"
                "roles,10,1,1,100,90,110,4,3,5,17,16,18\n",
                encoding="utf-8",
            )
            profile_root.joinpath("run.json").write_text(
                json.dumps(
                    {
                        "parameters": {"workload": {"type": "fake"}},
                        "workload_result_schema": self._custom_local_ydb_result_schema(queue_unit=queue_unit),
                    }
                ),
                encoding="utf-8",
            )

        value = chart_data(self.root, ["custom-one"], "local-ydb")
        self.assertIn("median_queue_depth", value["metrics"])
        self.assertEqual(value["metric_metadata"]["median_throughput"]["unit"], "widgets/s")
        self.assertEqual(value["metric_metadata"]["median_latency_ms"]["unit"], "ms")
        self.assertEqual(value["metric_metadata"]["median_queue_depth"]["unit"], "messages")
        self.assertEqual(value["metric_metadata"]["median_queue_depth"]["description"], "queued widgets")
        self.assertEqual(value["series"][0]["result_schema_id"], "fake-json-v1")

        conflicting = chart_data(self.root, ["custom-one", "custom-two"], "local-ydb")
        self.assertEqual(conflicting["metric_metadata"]["median_queue_depth"]["unit"], "varies")
        self.assertTrue(conflicting["metric_metadata"]["median_queue_depth"]["conflict"])

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
                self.assertIn(b"function loadLocalYdbActivity", script)
                self.assertIn(b"/local-ydb-activity?", script)
                self.assertIn(b"function localActivityLog", script)
                self.assertIn(b"Recent activity", script)
                self.assertIn(b"activityScrollTop", script)
                self.assertIn(b"activityPinned", script)
                self.assertIn(b"showLiveOutput=activeBenchmark!=='local-ydb'", script)
                self.assertIn(b"local-load-allow-errors", script)
                self.assertIn(b"allow-errors: ", script)
                self.assertIn(b"Failed workload requests are allowed", script)
                self.assertIn(b"editor.model?.local_ydb_workloads", script)
                self.assertIn(b"definition.load_parameters", script)
                self.assertIn(
                    b"config.load=localYdbLoadForWorkload(config.load,parameters,nextDefinition,config.workload)",
                    script,
                )
                self.assertIn(b"log:'batches/s'", script)
                self.assertIn(b"function localResultSchema", script)
                self.assertIn(b"result_schema_id:schema.schema_id", script)
                self.assertIn(b"const metricHeaders=metricColumns.map", script)
                self.assertIn(b"if(option.choices.length)return localSelect", script)
                self.assertIn(b"if(option.kind==='boolean')return localCheck", script)
                self.assertIn(b"if(option.kind==='integer')", script)
                self.assertIn(b"return localField(id,option.name,value,'','type=text')", script)
                self.assertIn(b"function yamlScalar(value)", script)
                self.assertIn(b"key+': '+yamlScalar(value)", script)
                self.assertIn(b"!option.allow_empty&&!value.length", script)
                self.assertNotIn(b"new RegExp(option.pattern)", script)
                self.assertNotIn(b"const localYdbOperations=", script)
                self.assertNotIn(b"type==='stock'?", script)
                self.assertIn(b"verification-repetitions:", script)
                self.assertIn(b"local-measurement-verification-repetitions", script)
                self.assertIn(b"localInteger('local-measurement-verification-repetitions',0,20)", script)
                self.assertIn(b"function localVerificationSummary", script)
                self.assertIn(b"metrics_source==='verification'", script)
                self.assertIn(b"verification-measuring", script)
                self.assertIn(b"'verification-evaluating':'Evaluating verification'", script)
                self.assertIn(b"Reported metrics come from the independent holdout", script)
                self.assertIn(b"Incompatible metric source", script)
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
                self.assertIn(b"sameMetricSource=metricView.source===baselineView.source", script)
                self.assertIn(b"<th>Metric source</th>", script)
                self.assertIn(b"function localComparisonKey", script)
                self.assertIn(b"Incompatible", script)
                self.assertIn(b"reference===0", script)
                self.assertIn(b"value===null", script)
                self.assertIn(b"Load values", script)
                self.assertIn(b"...Object.keys(config)", script)
                self.assertIn(b"series.benchmark!=='local-ydb'", script)
                self.assertIn(b"const curveMetrics=localComparisonCurveMetrics", script)
                self.assertIn(b"metric.repetition_aggregation==='sum'?'sum_':'median_'", script)
                self.assertIn(b"['errors','sum_errors','Errors across repetitions']", script)
                self.assertIn(b"localMetricLabel(schema,metric.name)", script)
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
                self.assertIn(b"const chartPointLimit=10000", script)
                self.assertIn(b"function chartExtent", script)
                self.assertIn(b"Chart omitted because it has more than", script)
                self.assertIn(b"Search curves omitted because they have more than", script)
                self.assertNotIn(b"Math.min(...values)", script)
                self.assertNotIn(b"Math.max(...values)", script)
                self.assertNotIn(b"Math.min(...numericX)", script)
                self.assertNotIn(b"Math.max(...numericX)", script)
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
                self.assertIn(b".local-activity-log{max-height:20rem;overflow:auto", stylesheet)
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
