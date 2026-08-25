"""Linux CPU sampling for benchmark process roles."""

import os
import threading
import time
from pathlib import Path

from ydb.tools.ydb_bench.lib.common import BenchmarkError


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
            self._records.append(record)
        self._previous_time = now
        self._previous_host = host
        self._previous_process = process_ticks

    def summary(self):
        def aggregate(name):
            samples = [
                (record[name], record["elapsed_seconds"])
                for record in self._records
                if name in record and record["elapsed_seconds"] > 0
            ]
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
