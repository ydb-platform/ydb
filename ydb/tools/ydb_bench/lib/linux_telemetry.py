"""Linux CPU sampling for benchmark process roles."""

import math
import os
import threading
import time
from pathlib import Path

from ydb.tools.ydb_bench.lib.common import BenchmarkError


def _is_finite_number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value)
    except (TypeError, ValueError, OverflowError):
        return False


class LogicalCpuSampler:
    """Shared, bounded on-demand sampler; requests within one second reuse a sample."""

    def __init__(self, proc_root=Path("/proc")):
        self.proc_root = Path(proc_root)
        self._lock = threading.Lock()
        self._previous = {}
        self._time = None
        self._result = None

    def sample(self):
        with self._lock:
            now = time.monotonic()
            if self._time is not None and now - self._time < 1:
                return self._result
            try:
                current = {}
                for line in self.proc_root.joinpath("stat").read_text().splitlines():
                    fields = line.split()
                    if fields and fields[0].startswith("cpu") and fields[0][3:].isdigit():
                        ticks = tuple(map(int, fields[1:9]))
                        if len(ticks) != 8 or any(value < 0 for value in ticks):
                            continue
                        current[int(fields[0][3:])] = ticks
            except (OSError, ValueError):
                current = {}
            cpus = {}
            for cpu, ticks in current.items():
                previous = self._previous.get(cpu)
                cpus[cpu] = None
                if previous is None:
                    continue
                delta = [value - old for value, old in zip(ticks, previous)]
                total = sum(delta)
                if total <= 0 or any(value < 0 for value in delta):
                    continue
                cpus[cpu] = {
                    "busy": 100 * (total - delta[3] - delta[4]) / total,
                    "user": 100 * (delta[0] + delta[1]) / total,
                    "system": 100 * (delta[2] + delta[5] + delta[6]) / total,
                    "iowait": 100 * delta[4] / total,
                    "steal": 100 * delta[7] / total,
                }
            self._result = {
                "cpus": cpus,
                "interval_seconds": None if self._time is None else now - self._time,
                "available": bool(current),
            }
            self._time = now
            self._previous = current
            return self._result


class LinuxCpuMonitor:
    def __init__(self, role_pids, role_cpu_counts, interval=0.5, proc_root=Path("/proc")):
        self.role_pids = role_pids
        self.role_cpu_counts = role_cpu_counts
        self.interval = interval
        self.proc_root = Path(proc_root)
        self.clock_ticks = os.sysconf("SC_CLK_TCK")
        self._stop = threading.Event()
        self._thread = None
        self._records = []
        self._previous_process = {}
        self._previous_host = None
        self._previous_time = None

    @property
    def records(self):
        return tuple(self._records)

    def start(self):
        if not self.proc_root.joinpath("stat").is_file():
            raise BenchmarkError("Linux /proc CPU statistics are unavailable")
        self._sample()
        self._thread = threading.Thread(target=self._run, name="ydb-bench-linux-cpu", daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(2.0, self.interval * 3))
        self._sample()
        return self.summary()

    def _run(self):
        while not self._stop.wait(self.interval):
            self._sample()

    def _read_process_ticks(self, pid):
        try:
            value = self.proc_root.joinpath(str(pid), "stat").read_text(encoding="utf-8")
            fields = value[value.rfind(")") + 2 :].split()
            return int(fields[11]) + int(fields[12])
        except (OSError, ValueError, IndexError):
            return None

    def _read_host_ticks(self):
        try:
            fields = self.proc_root.joinpath("stat").read_text(encoding="utf-8").splitlines()[0].split()
            values = [int(item) for item in fields[1:]]
        except (OSError, ValueError, IndexError):
            return None
        idle = values[3] + (values[4] if len(values) > 4 else 0)
        return sum(values), idle

    def _sample(self):
        now = time.monotonic()
        now_unix = time.time()
        host = self._read_host_ticks()
        process_ticks = {
            role: sum(ticks for pid in tuple(provider()) if (ticks := self._read_process_ticks(pid)) is not None)
            for role, provider in self.role_pids.items()
        }
        if self._previous_time is None:
            self._previous_time = now
            self._previous_host = host
            self._previous_process = process_ticks
            return

        elapsed = now - self._previous_time
        if elapsed <= 0:
            return
        record = {"elapsed_seconds": elapsed}
        for role, ticks in process_ticks.items():
            previous = self._previous_process.get(role)
            if previous is None or ticks < previous:
                continue
            raw_percent = 100.0 * (ticks - previous) / self.clock_ticks / elapsed
            capacity = max(1, self.role_cpu_counts.get(role, 1))
            record[role + "_cpu_raw"] = raw_percent
            record[role + "_cpu"] = min(100.0, raw_percent / capacity)
        if host is not None and self._previous_host is not None:
            total_delta = host[0] - self._previous_host[0]
            idle_delta = host[1] - self._previous_host[1]
            if total_delta > 0:
                record["host_cpu"] = 100.0 * (total_delta - idle_delta) / total_delta
        if len(record) > 1:
            record["timestamp_monotonic"] = now
            record["timestamp_unix"] = now_unix
            self._records.append(record)
        self._previous_time = now
        self._previous_host = host
        self._previous_process = process_ticks

    def summary(self, started_at_unix=None, finished_at_unix=None):
        windowed = started_at_unix is not None or finished_at_unix is not None
        if windowed:
            if started_at_unix is None or finished_at_unix is None:
                raise BenchmarkError("CPU measurement window requires both start and finish")
            if (
                not _is_finite_number(started_at_unix)
                or not _is_finite_number(finished_at_unix)
                or started_at_unix >= finished_at_unix
            ):
                raise BenchmarkError("CPU measurement window must be finite and increasing")

        def inside_measurement_window(record):
            if started_at_unix is None and finished_at_unix is None:
                return True
            timestamp = record.get("timestamp_unix")
            if timestamp is None:
                return False
            interval_started_at = timestamp - record["elapsed_seconds"]
            if started_at_unix is not None and interval_started_at < started_at_unix:
                return False
            if finished_at_unix is not None and timestamp > finished_at_unix:
                return False
            return True

        records = [
            record
            for record in self._records
            if record.get("elapsed_seconds", 0) > 0 and inside_measurement_window(record)
        ]
        if windowed and not records:
            raise BenchmarkError("CPU measurement window does not contain a complete sample interval")

        def aggregate(name):
            samples = [(record[name], record["elapsed_seconds"]) for record in records if name in record]
            if not samples:
                return 0.0, 0.0
            elapsed = sum(duration for _, duration in samples)
            mean = sum(value * duration for value, duration in samples) / elapsed
            stable = [value for value, duration in samples if duration >= self.interval * 0.5]
            return mean, max(stable) if stable else mean

        result = {}
        for role in self.role_pids:
            mean, maximum = aggregate(role + "_cpu")
            result[role + "_cpu_mean"] = mean
            result[role + "_cpu_max"] = maximum
        result["host_cpu_mean"], result["host_cpu_max"] = aggregate("host_cpu")
        return result
