"""Per-host sampling and clock-bounded CPU aggregation for a distributed run."""

import itertools
import math
import os
import threading
import time

from ydb.tools.ydb_bench.lib.common import BenchmarkError, atomic_write_json
from ydb.tools.ydb_bench.lib.distributed_artifacts import snapshot_results
from ydb.tools.ydb_bench.lib.linux_telemetry import LinuxCpuMonitor
from ydb.tools.ydb_bench.lib.ydb_telemetry import YdbCountersMonitor

MAX_CPU_RECORDS = 20000
MIN_CPU_COVERAGE = 0.8
CLOCK_DRIFT_TOLERANCE = 0.05


def finite(value):
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        return False


def role_capacities(prepared):
    allowed = set(prepared["topology"]["allowed_cpus"])
    result = {}
    for role in ("static", "dynamic", "cli"):
        nodes = [node for node in prepared["nodes"] if node["role"] == role]
        masks = [allowed if node["placement"]["cpus"] is None else set(node["placement"]["cpus"]) for node in nodes]
        result[role] = len(set().union(*masks))
    result["host"] = os.cpu_count() or len(allowed)
    return result


class WorkerTelemetry:
    def __init__(self, state, sample_id, context):
        self.state, self.sample_id = state, sample_id
        self.root = state["root"] / "results"
        self.directory = self.root / "telemetry" / sample_id
        self.capacities = role_capacities(state["prepared"])
        self._lock = threading.Lock()
        self._stopped = False
        self.cpu = LinuxCpuMonitor(
            {role: lambda role=role: self.pids(role) for role in ("static", "dynamic", "cli") if self.capacities[role]},
            self.capacities,
            max_records=MAX_CPU_RECORDS,
            stable_pids_only=True,
        )
        self.counters = YdbCountersMonitor(self.directory / "ydb-metrics.jsonl", self.nodes, context)

    def pids(self, role):
        # Called from monitor threads and shutdown while the admission lock is
        # held. Take a snapshot, never acquire the admission lock here.
        processes = dict(self.state["processes"])
        return tuple(
            processes[node["name"]].pid
            for node in self.state["prepared"]["nodes"]
            if node["role"] == role and node["name"] in processes and processes[node["name"]].poll() is None
        )

    def nodes(self):
        return [
            (node["role"], node["node_id"], node["ports"]["mon_port"])
            for node in self.state["prepared"]["nodes"]
            if node["role"] != "cli"
        ]

    def start(self):
        with self._lock:
            if self._stopped:
                raise BenchmarkError("Distributed telemetry was already stopped")
            self.cpu.start()
            try:
                self.counters.start()
            except BaseException:
                self.cpu.stop()
                raise

    def stop(self):
        with self._lock:
            if self._stopped:
                return
            self._stopped = True
            try:
                self.cpu.stop()
            finally:
                self.counters.stop()

    def finish(self):
        self.stop()
        record = {
            "sample_id": self.sample_id,
            "capacities": self.capacities,
            "samples": list(self.cpu.records),
            "truncated": self.cpu.truncated,
            "counters_error": self.counters.error,
        }
        atomic_write_json(self.directory / "cpu-samples.json", record)
        return {"sample_id": self.sample_id, "artifacts": snapshot_results(self.root, self.directory, telemetry=True)}


def estimate_clock(call, samples=3):
    estimates = []
    for _ in range(samples):
        before = time.monotonic()
        clock = call()
        after = time.monotonic()
        if not isinstance(clock, dict) or not finite(clock.get("monotonic")) or after < before:
            raise BenchmarkError("Invalid distributed clock response")
        estimates.append(
            {
                "offset": clock["monotonic"] - (before + after) / 2,
                "uncertainty": (after - before) / 2,
                "coordinator_monotonic": (before + after) / 2,
            }
        )
    return min(estimates, key=lambda item: item["uncertainty"])


def combined_clock(before, after):
    if not all(finite(item.get(name)) for item in (before, after) for name in ("offset", "uncertainty")):
        raise BenchmarkError("Invalid CPU clock estimate")
    drift = abs(before["offset"] - after["offset"])
    if drift > before["uncertainty"] + after["uncertainty"] + CLOCK_DRIFT_TOLERANCE:
        raise BenchmarkError("Worker clock changed beyond the measured uncertainty")
    return {
        "offset": (before["offset"] + after["offset"]) / 2,
        "uncertainty": max(before["uncertainty"], after["uncertainty"]) + drift / 2,
    }


def _role_summary(hosts, role, clocks, window):
    capacities = {host: data["capacities"][role] for host, data in hosts.items() if data["capacities"].get(role, 0)}
    if not capacities:
        return {"coverage": 0.0, "error": "No participating nodes for this role"}
    events = []
    for host, capacity in capacities.items():
        if not finite(capacity) or capacity <= 0:
            raise BenchmarkError("Invalid distributed CPU capacity")
        clock = clocks[host]
        previous = None
        for record in hosts[host]["samples"]:
            end, duration, value = (
                record.get("timestamp_monotonic"),
                record.get("elapsed_seconds"),
                record.get(role + "_cpu"),
            )
            if not all(finite(item) for item in (end, duration, value)) or duration <= 0 or not 0 <= value <= 100:
                continue
            start = end - duration
            if previous is not None and start < previous - 1e-6:
                raise BenchmarkError("Overlapping distributed CPU sample intervals")
            previous = end
            lo = max(window[0], start - clock["offset"] + clock["uncertainty"])
            hi = min(window[1], end - clock["offset"] - clock["uncertainty"])
            if hi > lo:
                events.extend(((lo, 1, host, value), (hi, 0, host, value)))
    current, covered, area, peak = {}, 0.0, 0.0, 0.0
    previous = window[0]
    capacity = sum(capacities.values())
    for timestamp, changes in itertools.groupby(sorted(events), key=lambda event: event[0]):
        elapsed = timestamp - previous
        if len(current) == len(capacities) and elapsed > 0:
            value = sum(current[host] * capacities[host] for host in capacities) / capacity
            covered += elapsed
            area += value * elapsed
            peak = max(peak, value)
        for _, kind, host, value in changes:
            if kind:
                current[host] = value
            else:
                current.pop(host, None)
        previous = timestamp
    coverage = covered / (window[1] - window[0])
    result = {"coverage": coverage, "covered_seconds": covered, "capacity": capacity}
    if coverage < MIN_CPU_COVERAGE:
        return {**result, "error": "Less than 80% common CPU measurement coverage"}
    return {**result, "mean": area / covered, "max": peak}


def summarize_hosts(hosts, cli_host, measurement_clock, before, after):
    report = {"metrics": {}, "roles": {}, "hosts": {}, "clock_estimates": {}}
    try:
        report["hosts"] = {
            host: {"capacities": data["capacities"], "counters_error": data.get("counters_error"), "roles": {}}
            for host, data in hosts.items()
        }
        if not isinstance(measurement_clock, dict):
            raise BenchmarkError("CLI measurement clock is unavailable")
        if measurement_clock.get("error"):
            raise BenchmarkError(measurement_clock["error"])
        start, end = measurement_clock.get("started_monotonic"), measurement_clock.get("finished_monotonic")
        if not finite(start) or not finite(end) or end <= start:
            raise BenchmarkError("Invalid CLI CPU measurement window")
        clocks = {host: combined_clock(before[host], after[host]) for host in hosts}
        report["clock_estimates"] = clocks
        reference = clocks[cli_host]
        window = (
            start - reference["offset"] + reference["uncertainty"],
            end - reference["offset"] - reference["uncertainty"],
        )
        if window[1] <= window[0]:
            raise BenchmarkError("Clock uncertainty is larger than the measurement window")
        report["measurement_window_coordinator_monotonic"] = list(window)
        for host, data in hosts.items():
            if data.get("truncated"):
                raise BenchmarkError("CPU samples were truncated on " + host)
            report["hosts"][host] = {
                "capacities": data["capacities"],
                "roles": {
                    role: _role_summary({host: data}, role, clocks, window)
                    for role in data["capacities"]
                    if data["capacities"][role]
                },
                "counters_error": data.get("counters_error"),
            }
        for role in ("static", "dynamic", "cli", "host"):
            summary = _role_summary(hosts, role, clocks, window)
            report["roles"][role] = summary
            if "error" not in summary:
                for suffix in ("mean", "max"):
                    report["metrics"][role + "_cpu_" + suffix] = summary[suffix]
    except (BenchmarkError, KeyError, TypeError, ValueError) as error:
        report["error"] = str(error)
        report["metrics"] = {}
    return report
