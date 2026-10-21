import copy
import unittest
from unittest import mock

from ydb.tools.ydb_bench.benchmarks import LOCAL_YDB_BENCHMARK
from ydb.tools.ydb_bench.lib import distributed_telemetry, linux_telemetry, local_ydb
from ydb.tools.ydb_bench.lib.common import BenchmarkError


class DistributedTelemetryTest(unittest.TestCase):
    def setUp(self):
        self.clocks = {host: {"offset": offset, "uncertainty": 0.0} for host, offset in (("a", 100), ("b", 200))}
        self.window = {"started_monotonic": 110, "finished_monotonic": 112}
        self.hosts = {
            "a": {
                "capacities": {"static": 4, "dynamic": 0, "cli": 2, "host": 8},
                "samples": [self.record(111, static=100, cli=20, host=50), self.record(112, static=0, cli=20, host=50)],
            },
            "b": {
                "capacities": {"static": 4, "dynamic": 8, "cli": 0, "host": 32},
                "samples": [
                    self.record(211, static=0, dynamic=50, host=25),
                    self.record(212, static=100, dynamic=50, host=25),
                ],
            },
        }

    def record(self, timestamp, **roles):
        return {
            "timestamp_monotonic": timestamp,
            "elapsed_seconds": 1.0,
            **{role + "_cpu": value for role, value in roles.items()},
        }

    def summarize(self, after=None):
        return distributed_telemetry.summarize_hosts(self.hosts, "a", self.window, self.clocks, after or self.clocks)

    def test_aligned_peaks_are_not_sums_of_per_host_peaks(self):
        result = self.summarize()
        self.assertNotIn("error", result)
        self.assertEqual(
            {
                "static_cpu_mean": 50.0,
                "static_cpu_max": 50.0,
                "dynamic_cpu_mean": 50.0,
                "dynamic_cpu_max": 50.0,
                "cli_cpu_mean": 20.0,
                "cli_cpu_max": 20.0,
                "host_cpu_mean": 30.0,
                "host_cpu_max": 30.0,
            },
            result["metrics"],
        )
        self.assertEqual(100, result["hosts"]["a"]["roles"]["static"]["max"])
        self.assertEqual(100, result["hosts"]["b"]["roles"]["static"]["max"])

    def test_role_aggregation_uses_assigned_capacity(self):
        self.hosts["b"]["capacities"]["static"] = 12
        result = self.summarize()
        self.assertEqual(50, result["metrics"]["static_cpu_mean"])
        self.assertEqual(75, result["metrics"]["static_cpu_max"])
        self.assertEqual(16, result["roles"]["static"]["capacity"])

    def test_missing_intervals_do_not_turn_into_zero_cpu(self):
        del self.hosts["b"]["samples"][1]["static_cpu"]
        result = self.summarize()
        self.assertNotIn("static_cpu_mean", result["metrics"])
        self.assertEqual(0.5, result["roles"]["static"]["coverage"])
        self.assertIn("error", result["roles"]["static"])
        self.assertEqual(50, result["metrics"]["dynamic_cpu_mean"])

    def test_changed_clock_and_truncated_samples_are_reported(self):
        after = copy.deepcopy(self.clocks)
        after["b"]["offset"] += 1
        result = self.summarize(after)
        self.assertEqual({}, result["metrics"])
        self.assertIn("clock changed", result["error"])
        self.hosts["b"]["truncated"] = True
        result = self.summarize()
        self.assertEqual({}, result["metrics"])
        self.assertIn("truncated on b", result["error"])

    def test_clock_uncertainty_reduces_coverage(self):
        self.clocks["b"]["uncertainty"] = 0.2
        result = self.summarize()
        self.assertAlmostEqual(0.6, result["roles"]["static"]["coverage"])
        self.assertNotIn("static_cpu_mean", result["metrics"])
        self.assertEqual(20, result["metrics"]["cli_cpu_mean"])

    def test_overlapping_records_are_not_double_counted(self):
        self.hosts["a"]["samples"].append(self.record(111.5, static=100, cli=20, host=50))
        result = self.summarize()
        self.assertEqual({}, result["metrics"])
        self.assertIn("Overlapping", result["error"])

    def test_missing_cli_clock_keeps_metrics_unavailable(self):
        self.window = {"error": "CLI wall clock changed during measurement"}
        result = self.summarize()
        self.assertEqual({}, result["metrics"])
        self.assertEqual(self.window["error"], result["error"])

    def test_probe_uses_best_round_trip_bound(self):
        responses = iter([{"monotonic": 300.05}, {"monotonic": 301.01}, {"monotonic": 302.1}])
        with mock.patch.object(distributed_telemetry.time, "monotonic", side_effect=[0, 0.1, 1, 1.02, 2, 2.2]):
            result = distributed_telemetry.estimate_clock(lambda: next(responses))
        self.assertAlmostEqual(300, result["offset"])
        self.assertAlmostEqual(0.01, result["uncertainty"])

    def test_capacities_union_shared_masks_and_ignore_actor_cpu_count(self):
        prepared = {
            "topology": {"allowed_cpus": list(range(32))},
            "nodes": [
                {"role": "static", "placement": {"cpus": list(range(16)), "reserved_cpus": list(range(8))}},
                {"role": "static", "placement": {"cpus": list(range(16)), "reserved_cpus": list(range(8, 16))}},
                {"role": "dynamic", "placement": {"cpus": None}},
                {"role": "cli", "placement": {"cpus": [30, 31]}},
            ],
        }
        with mock.patch.object(distributed_telemetry.os, "cpu_count", return_value=64):
            result = distributed_telemetry.role_capacities(prepared)
        self.assertEqual({"static": 16, "dynamic": 32, "cli": 2, "host": 64}, result)

    def test_repetitions_omit_partial_cpu_but_still_require_workload_schema(self):
        rows = [
            {"load": 1, "dynamic_nodes": 1, "throughput": 10, "static_cpu_mean": 40},
            {"load": 1, "dynamic_nodes": 1, "throughput": 20},
        ]
        result = local_ydb._aggregate_measurements(rows)
        self.assertEqual(15, result["throughput"])
        self.assertNotIn("static_cpu_mean", result)
        summary = LOCAL_YDB_BENCHMARK.summarize_metrics(rows, LOCAL_YDB_BENCHMARK)
        self.assertEqual(15, summary[0]["median_throughput"])
        self.assertNotIn("median_static_cpu_mean", summary[0])
        rows[1]["p99_ms"] = 5
        with self.assertRaisesRegex(BenchmarkError, "inconsistent metric keys"):
            local_ydb._aggregate_measurements(rows)
        with self.assertRaisesRegex(BenchmarkError, "inconsistent metric keys"):
            LOCAL_YDB_BENCHMARK.summarize_metrics(rows, LOCAL_YDB_BENCHMARK)

    def test_sampler_bounds_memory_and_does_not_subtract_different_pids(self):
        pid, ticks = [1], {1: 100, 2: 1000}
        monitor = linux_telemetry.LinuxCpuMonitor(
            {"cli": lambda: pid}, {"cli": 4}, max_records=1, stable_pids_only=True
        )
        monitor.clock_ticks = 100
        with mock.patch.object(monitor, "_read_host_ticks", return_value=None), mock.patch.object(
            monitor, "_read_process_ticks", side_effect=lambda value: ticks[value]
        ), mock.patch.object(linux_telemetry.time, "monotonic", side_effect=[10, 11, 12, 13]):
            monitor._sample()
            ticks[1] = 200
            monitor._sample()
            pid[:] = [2]
            monitor._sample()
            ticks[2] = 1100
            monitor._sample()
        self.assertEqual(1, len(monitor.records))
        self.assertTrue(monitor.truncated)
        self.assertEqual(25, monitor.records[0]["cli_cpu"])
