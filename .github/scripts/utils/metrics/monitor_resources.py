#!/usr/bin/env python3
"""Sample CPU/RAM/disk while ya make runs; write JSONL for the tests dashboard."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path


def _parse_proc_stat(line: str) -> tuple[float, float, float]:
    """Parse the aggregate /proc/stat line into total, idle, and iowait jiffies."""
    parts = line.split()
    # user nice system idle iowait irq softirq steal. guest and guest_nice are already inside user and nice.
    total = sum(int(x) for x in parts[1:9])
    idle = int(parts[4]) if len(parts) > 4 else 0
    iowait = int(parts[5]) if len(parts) > 5 else 0
    return float(total), float(idle + iowait), float(iowait)


def read_proc_stat() -> tuple[float, float, float]:
    """Read /proc/stat, return (total_jiffies, idle_jiffies, iowait_jiffies)."""
    with open("/proc/stat") as f:
        return _parse_proc_stat(f.readline())


def _parse_pressure(text: str) -> dict[str, int]:
    """Read cumulative PSI stall time in microseconds by some/full class."""
    totals: dict[str, int] = {}
    for line in text.splitlines():
        parts = line.split()
        if not parts or parts[0] not in ("some", "full"):
            continue
        for item in parts[1:]:
            if item.startswith("total="):
                try:
                    totals[parts[0]] = int(item.split("=", 1)[1])
                except ValueError:
                    pass
                break
    return totals


def read_host_pressure() -> dict[str, dict[str, int]]:
    """Read host CPU, memory, and I/O PSI counters when the kernel exposes them."""
    pressure: dict[str, dict[str, int]] = {}
    for resource in ("cpu", "memory", "io"):
        try:
            pressure[resource] = _parse_pressure(Path("/proc/pressure", resource).read_text())
        except OSError:
            pressure[resource] = {}
    return pressure


def pressure_interval_pct(
    current: dict[str, dict[str, int]],
    previous: dict[str, dict[str, int]],
    dt_sec: float,
) -> dict[str, float]:
    """Convert available PSI cumulative totals to per-interval stall percentages."""
    if dt_sec <= 0:
        return {}
    result: dict[str, float] = {}
    for resource in ("cpu", "memory", "io"):
        for scope in ("some", "full"):
            current_total = current.get(resource, {}).get(scope)
            previous_total = previous.get(resource, {}).get(scope)
            if current_total is None or previous_total is None:
                continue
            delta_usec = max(0, current_total - previous_total)
            result[f"pressure_{resource}_{scope}_pct"] = round(delta_usec / (dt_sec * 10_000), 2)
    return result


def _read_cmdline(pid: int) -> str:
    try:
        return (Path("/proc") / str(pid) / "cmdline").read_text().replace("\x00", " ")
    except (OSError, ValueError):
        return ""


def _read_cwd(pid: int) -> str:
    try:
        return os.readlink(f"/proc/{pid}/cwd")
    except OSError:
        return ""


def _is_ya_root(cmdline: str, comm: str) -> bool:
    """Process is a ya root (ya, ya-tc, ya.make). Match by executable basename or comm."""
    ya_names = ("ya", "ya-tc", "ya.make", "ya-bin")
    c = (comm or "").lower()
    if c in ya_names:
        return True
    cmd = (cmdline or "").strip()
    if not cmd:
        return False
    parts = cmd.split()
    exe = parts[0].lower() if parts else ""
    exe_basename = exe.rsplit("/", 1)[-1] if exe else ""
    if exe_basename in ya_names:
        return True
    # Fallback: ya invoked via python/wrapper (e.g. "python3 /path/to/ya make")
    for part in parts[1:3]:  # check first few args
        p = (part or "").lower()
        if p.endswith("/ya") or p.endswith("/ya-tc") or p.endswith("/ya.make") or p.endswith("/ya-bin"):
            return True
    return False


def read_process_snapshot() -> dict[int, dict]:
    """Read one /proc snapshot with process parent, command, CPU and RSS fields."""
    proc = Path("/proc")
    snapshot: dict[int, dict] = {}
    try:
        page_size_kb = os.sysconf("SC_PAGE_SIZE") // 1024
    except (OSError, ValueError):
        page_size_kb = 4
    if page_size_kb <= 0:
        page_size_kb = 4
    for pid_dir in proc.iterdir():
        if not pid_dir.is_dir() or not pid_dir.name.isdigit():
            continue
        pid = int(pid_dir.name)
        try:
            stat = (pid_dir / "stat").read_text()
            rparen = stat.rfind(")")
            if rparen < 0:
                continue
            comm = stat[stat.find("(") + 1 : rparen]
            rest = stat[rparen + 2 :].split()
            if len(rest) < 24:
                continue
            ppid = int(rest[1])
            utime = int(rest[11])
            stime = int(rest[12])
            starttime = int(rest[19]) if len(rest) > 19 else 0
            rss_pages = int(rest[21])
            snapshot[pid] = {
                "pid": pid,
                "ppid": ppid,
                "comm": comm[:80],
                "cmdline": _read_cmdline(pid),
                "utime": utime,
                "stime": stime,
                "starttime": starttime,
                "rss_kb": rss_pages * page_size_kb,
            }
        except (OSError, ValueError):
            continue
    return snapshot


def find_ya_process_tree(processes: dict[int, dict]) -> set[int]:
    """Find ya make PIDs as detected ya roots plus their descendants."""
    roots = {
        pid
        for pid, proc in processes.items()
        if _is_ya_root(str(proc.get("cmdline", "")), str(proc.get("comm", "")))
    }
    ya_pids: set[int] = set(roots)
    # Include only descendants of actual ya roots. Adding arbitrary ancestors (shell,
    # runner, systemd scope) can pull unrelated sibling processes into ya-only metrics.
    changed = True
    while changed:
        changed = False
        for pid, proc in processes.items():
            ppid = int(proc.get("ppid") or 0)
            if pid not in ya_pids and ppid in ya_pids:
                ya_pids.add(pid)
                changed = True
    return ya_pids


def process_identity(proc: dict) -> tuple[int, int]:
    return (int(proc["pid"]), int(proc.get("starttime") or 0))


def cpu_delta_jiffies(prev: tuple[int, int] | None, utime: int, stime: int) -> int:
    """Interval CPU for a PID. New processes contribute their lifetime so far."""
    if prev is None:
        return max(int(utime) + int(stime), 0)
    return max((int(utime) - prev[0]) + (int(stime) - prev[1]), 0)


def io_delta_bytes(prev: tuple[int, int] | None, curr: tuple[int, int]) -> tuple[int, int]:
    """Interval I/O for one PID. New processes contribute their lifetime counters."""
    if prev is None:
        return (max(curr[0], 0), max(curr[1], 0))
    return (max(curr[0] - prev[0], 0), max(curr[1] - prev[1], 0))


_IO_UNSEEN = object()


def apply_io_sample(
    ident: tuple[int, int],
    curr: tuple[int, int] | None,
    prev_pid_io: dict[tuple[int, int], tuple[int, int] | None],
    next_pid_io: dict[tuple[int, int], tuple[int, int] | None],
) -> tuple[int, int]:
    """Interval read/write for one process sample.

    A failed /proc read is stored as None, not (0, 0). The next successful read
    then starts a baseline instead of counting the process lifetime as one
    interval. A process seen for the first time with a good read still
    contributes its lifetime counters.
    """
    if curr is None:
        if ident in prev_pid_io:
            next_pid_io[ident] = prev_pid_io[ident]
        else:
            next_pid_io[ident] = None
        return (0, 0)
    prev = prev_pid_io.get(ident, _IO_UNSEEN)
    next_pid_io[ident] = curr
    if prev is _IO_UNSEEN:
        return io_delta_bytes(None, curr)
    if prev is None:
        return (0, 0)
    return io_delta_bytes(prev, curr)


def seed_ya_counters(processes: dict[int, dict], ya_pids: set[int]) -> tuple[dict[tuple[int, int], tuple[int, int]], dict[tuple[int, int], tuple[int, int] | None]]:
    cpu: dict[tuple[int, int], tuple[int, int]] = {}
    io: dict[tuple[int, int], tuple[int, int] | None] = {}
    for pid in ya_pids:
        proc = processes.get(pid)
        if not proc:
            continue
        ident = process_identity(proc)
        cpu[ident] = (int(proc.get("utime") or 0), int(proc.get("stime") or 0))
        io[ident] = get_process_io(pid)
    return cpu, io


def get_process_io(pid: int) -> tuple[int, int] | None:
    """read_bytes, write_bytes from /proc/pid/io. None if the file cannot be read."""
    try:
        io = (Path("/proc") / str(pid) / "io").read_text()
        r, w = 0, 0
        for line in io.splitlines():
            if line.startswith("read_bytes:"):
                r = int(line.split(":")[1].strip())
            elif line.startswith("write_bytes:"):
                w = int(line.split(":")[1].strip())
        return r, w
    except (OSError, ValueError):
        return None


TEST_CGROUP_PREFIX = "ya-test-"
SUITE_ROOTS = ("ydb/", "library/", "contrib/", "devtools/", "kikimr/")
GENERIC_SUITE_NAMES = frozenset(
    {
        "unittest",
        "py3test",
        "pytest",
        "gtest",
        "exectest",
        "fuzz",
        "go_test",
        "java",
        "playwright",
        "unknown",
    }
)
CHUNK_IN_PATH_RE = re.compile(r"(?:^|/)chunk(\d+)(?:/|$)")


def is_generic_suite(name: str | None) -> bool:
    text = str(name or "").strip()
    if not text:
        return True
    lower = text.lower()
    return lower in GENERIC_SUITE_NAMES or lower.startswith(TEST_CGROUP_PREFIX)


def identity_quality(ident: dict) -> int:
    quality = 0
    if not is_generic_suite(ident.get("suite")):
        quality += 2
    if ident.get("chunk") is not None:
        quality += 1
    return quality


def chunk_from_text(text: str) -> int | None:
    if not text:
        return None
    match = CHUNK_IN_PATH_RE.search(text)
    return int(match.group(1)) if match else None


def _suite_after_root(text: str, drop_last: bool) -> str | None:
    if not text:
        return None
    best_pos = -1
    used_root: str | None = None
    for root in SUITE_ROOTS:
        pos = text.rfind("/" + root)
        if pos != -1 and pos > best_pos:
            best_pos = pos
            used_root = root
        elif pos == -1 and text.startswith(root) and best_pos < 0:
            best_pos = 0
            used_root = root
    if used_root is None:
        return None
    candidate = text if best_pos == 0 and text.startswith(used_root) else text[best_pos + 1 :]
    parts = [part for part in candidate.split("/") if part]
    if drop_last and len(parts) >= 2:
        parts = parts[:-1]
    if len(parts) < 2:
        return None
    return "/".join(parts)


def suite_from_workdir(path: str) -> str | None:
    if not path:
        return None
    marker = path.find("/test-results/")
    if marker == -1:
        marker = path.find("/test-results")
    search = path[:marker] if marker != -1 else path
    return _suite_after_root(search, drop_last=False)


def suite_from_exe(path: str) -> str | None:
    if not path:
        return None
    return _suite_after_root(path.split()[0], drop_last=True)


def parse_cmdline_flag(cmdline: str, flag: str) -> str | None:
    """Return the value of `--flag value` or `--flag=value` in a cmdline string."""
    parts = (cmdline or "").split()
    prefix = flag + "="
    for i, part in enumerate(parts):
        if part == flag and i + 1 < len(parts):
            return parts[i + 1]
        if part.startswith(prefix):
            return part[len(prefix) :]
    return None


def read_environ(pid: int) -> dict[str, str]:
    try:
        raw = (Path("/proc") / str(pid) / "environ").read_bytes()
    except OSError:
        return {}
    env: dict[str, str] = {}
    for item in raw.split(b"\x00"):
        if not item or b"=" not in item:
            continue
        key, value = item.split(b"=", 1)
        env[key.decode("utf-8", "replace")] = value.decode("utf-8", "replace")
    return env


def read_cgroup_procs(cgroup: Path) -> list[int]:
    try:
        text = (cgroup / "cgroup.procs").read_text()
    except OSError:
        return []
    pids: list[int] = []
    for line in text.splitlines():
        line = line.strip()
        if line.isdigit():
            pids.append(int(line))
    return pids


def read_cpu_usage_usec(cgroup: Path) -> int | None:
    try:
        for line in (cgroup / "cpu.stat").read_text().splitlines():
            if line.startswith("usage_usec"):
                return int(line.split()[1])
    except (OSError, ValueError, IndexError):
        return None
    return None


def _read_key_value_file(path: Path) -> dict[str, int] | None:
    """Read cgroup v2 key/value counters. Missing or malformed files are unavailable."""
    try:
        rows: dict[str, int] = {}
        for line in path.read_text().splitlines():
            parts = line.split()
            if len(parts) >= 2:
                rows[parts[0]] = int(parts[1])
        return rows
    except (OSError, ValueError):
        return None


def read_cpu_stat(cgroup: Path) -> dict[str, int] | None:
    return _read_key_value_file(cgroup / "cpu.stat")


def read_memory_events(cgroup: Path) -> dict[str, int] | None:
    return _read_key_value_file(cgroup / "memory.events")


def read_psi_total_usec(cgroup: Path, resource: str) -> dict[str, int] | None:
    """Return cumulative PSI total_usec counters by pressure class (some/full)."""
    try:
        text = (cgroup / f"{resource}.pressure").read_text()
    except OSError:
        return None
    totals: dict[str, int] = {}
    try:
        for line in text.splitlines():
            parts = line.split()
            if not parts:
                continue
            for field in parts[1:]:
                if field.startswith("total="):
                    totals[parts[0]] = int(field.split("=", 1)[1])
                    break
    except ValueError:
        return None
    return totals


def read_memory_current_bytes(cgroup: Path) -> int | None:
    try:
        return int((cgroup / "memory.current").read_text().strip())
    except (OSError, ValueError):
        return None


def read_io_bytes(cgroup: Path) -> tuple[int, int] | None:
    """Sum rbytes/wbytes from cgroup v2 io.stat. None if the file is missing."""
    try:
        text = (cgroup / "io.stat").read_text()
    except OSError:
        return None
    read_b = 0
    write_b = 0
    found = False
    for line in text.splitlines():
        for tok in line.split():
            if tok.startswith("rbytes="):
                read_b += int(tok.split("=", 1)[1])
                found = True
            elif tok.startswith("wbytes="):
                write_b += int(tok.split("=", 1)[1])
                found = True
    if not found:
        return (0, 0)
    return read_b, write_b


def discover_test_cgroups(extra_root: Path | None = None) -> list[Path]:
    """Find ya-test-* cgroups created by --limit-test-cpu."""
    roots: list[Path] = []
    env_root = os.environ.get("YA_CPU_CGROUP_ROOT")
    if env_root:
        roots.append(Path(env_root))
    if extra_root is not None:
        roots.append(extra_root)
    roots.append(Path("/sys/fs/cgroup"))
    found: list[Path] = []
    seen: set[str] = set()
    for base in roots:
        if not base.is_dir():
            continue
        try:
            for dirpath, dirnames, _filenames in os.walk(base, followlinks=False):
                name = os.path.basename(dirpath)
                if name.startswith(TEST_CGROUP_PREFIX):
                    resolved = os.path.realpath(dirpath)
                    if resolved not in seen:
                        seen.add(resolved)
                        found.append(Path(dirpath))
                    dirnames.clear()
        except OSError:
            continue
    return found


def identify_test_cgroup(cgroup: Path) -> dict:
    """Resolve suite/chunk from test_tool flags, cwd, or the unittest binary path."""
    identity = {
        "cgroup": cgroup.name,
        "suite": cgroup.name,
        "chunk": None,
        "test_name": None,
        "uid": None,
    }
    for pid in read_cgroup_procs(cgroup):
        cmdline = _read_cmdline(pid)
        env = read_environ(pid)
        cwd = _read_cwd(pid)
        project = parse_cmdline_flag(cmdline, "--project-path")
        split = parse_cmdline_flag(cmdline, "--split-index")
        suite_name = parse_cmdline_flag(cmdline, "--test-suite-name") or env.get("TEST_NAME")
        uid = env.get("TEST_NODE_SUITE_UID")
        if project and not is_generic_suite(project):
            identity["suite"] = project
        elif is_generic_suite(identity.get("suite")):
            path_suite = suite_from_workdir(cwd) or suite_from_exe(cmdline)
            if path_suite:
                identity["suite"] = path_suite
        if split is not None and split.lstrip("-").isdigit():
            identity["chunk"] = int(split)
        elif identity.get("chunk") is None:
            path_chunk = chunk_from_text(cwd) or chunk_from_text(cmdline)
            if path_chunk is not None:
                identity["chunk"] = path_chunk
        if suite_name:
            identity["test_name"] = suite_name
        if uid:
            identity["uid"] = uid
        if identity_quality(identity) >= 3:
            return identity
    return identity


class TestCgroupSampler:
    """Per-cgroup interval CPU/RAM/disk. First sighting is a baseline."""

    def __init__(self) -> None:
        self.prev_cpu_usec: dict[str, int] = {}
        self.prev_io: dict[str, tuple[int, int]] = {}
        self.prev_pid_io: dict[tuple[str, tuple[int, int]], tuple[int, int] | None] = {}
        self.prev_counters: dict[str, dict[str, int]] = {}
        self.identity: dict[str, dict] = {}

    def sample(
        self,
        dt: float,
        ts: float,
        ts_us: int,
        process_snapshot: dict[int, dict],
        extra_root: Path | None = None,
    ) -> list[dict]:
        records: list[dict] = []
        hz = 100
        try:
            hz = int(os.sysconf("SC_CLK_TCK") or 100)
        except (OSError, ValueError):
            hz = 100
        if hz <= 0:
            hz = 100
        for cgroup in discover_test_cgroups(extra_root):
            rec = self._sample_one(cgroup, dt, ts, ts_us, process_snapshot, hz)
            if rec is not None:
                records.append(rec)
        return records

    def _sample_one(
        self,
        cgroup: Path,
        dt: float,
        ts: float,
        ts_us: int,
        process_snapshot: dict[int, dict],
        hz: int,
    ) -> dict | None:
        key = str(cgroup)
        ident = self.identity.get(key)
        if ident is None or identity_quality(ident) < 3:
            fresh = identify_test_cgroup(cgroup)
            if ident is None or identity_quality(fresh) > identity_quality(ident):
                self.identity[key] = fresh
                ident = fresh
        pids = read_cgroup_procs(cgroup)

        cpu_usec = read_cpu_usage_usec(cgroup)
        if cpu_usec is None:
            cpu_jiffies = 0
            for pid in pids:
                proc = process_snapshot.get(pid)
                if not proc:
                    continue
                cpu_jiffies += int(proc.get("utime") or 0) + int(proc.get("stime") or 0)
            cpu_usec = int(cpu_jiffies * 1_000_000 / hz)

        mem_b = read_memory_current_bytes(cgroup)
        if mem_b is None:
            rss_kb = 0
            for pid in pids:
                proc = process_snapshot.get(pid)
                if proc:
                    rss_kb += int(proc.get("rss_kb") or 0)
            mem_b = rss_kb * 1024

        io_curr = read_io_bytes(cgroup)
        counter_curr: dict[str, int] = {}
        cpu_stat = read_cpu_stat(cgroup)
        if cpu_stat is not None:
            counter_curr.update({f"cpu_stat_{name}": value for name, value in cpu_stat.items()})
        memory_events = read_memory_events(cgroup)
        if memory_events is not None:
            counter_curr.update({f"memory_events_{name}": value for name, value in memory_events.items()})
        for resource in ("cpu", "memory", "io"):
            psi = read_psi_total_usec(cgroup, resource)
            if psi is not None:
                counter_curr.update({f"{resource}_psi_{kind}_total_usec": value for kind, value in psi.items()})
        if io_curr is None:
            read_b = 0
            write_b = 0
            next_pid_io: dict[tuple[str, tuple[int, int]], tuple[int, int] | None] = {}
            for pid in pids:
                proc = process_snapshot.get(pid)
                if not proc:
                    continue
                pid_ident = (key, process_identity(proc))
                delta_r, delta_w = apply_io_sample(
                    pid_ident,
                    get_process_io(pid),
                    self.prev_pid_io,
                    next_pid_io,
                )
                read_b += delta_r
                write_b += delta_w
            self.prev_pid_io = {k: v for k, v in self.prev_pid_io.items() if k[0] != key}
            self.prev_pid_io.update(next_pid_io)
            io_delta = (read_b, write_b)
            io_ready = key in self.prev_cpu_usec
        else:
            prev_io = self.prev_io.get(key)
            self.prev_io[key] = io_curr
            if prev_io is None:
                io_delta = (0, 0)
                io_ready = False
            else:
                io_delta = io_delta_bytes(prev_io, io_curr)
                io_ready = True

        prev_cpu = self.prev_cpu_usec.get(key)
        self.prev_cpu_usec[key] = cpu_usec
        prev_counters = self.prev_counters.get(key)
        self.prev_counters[key] = counter_curr
        if prev_cpu is None:
            return None
        if dt <= 0:
            dt = 1.0
        cpu_cores = max(cpu_usec - prev_cpu, 0) / dt / 1_000_000.0
        if not io_ready:
            io_delta = (0, 0)
        read_mb = io_delta[0] / (1024 * 1024)
        write_mb = io_delta[1] / (1024 * 1024)
        record = {
            "kind": "test",
            "ts": round(ts, 3),
            "ts_us": ts_us,
            "dt_sec": round(dt, 6),
            "cgroup": ident.get("cgroup"),
            "suite": ident.get("suite"),
            "chunk": ident.get("chunk"),
            "test_name": ident.get("test_name"),
            "uid": ident.get("uid"),
            "cpu_used_cores": round(cpu_cores, 3),
            "ram_kb": int(mem_b / 1024),
            "disk_read_mb_delta": round(read_mb, 2),
            "disk_write_mb_delta": round(write_mb, 2),
            "disk_read_mbps": round(read_mb / dt, 2),
            "disk_write_mbps": round(write_mb / dt, 2),
        }
        if prev_counters is not None:
            dt_usec = dt * 1_000_000.0

            def counter_delta(name: str) -> int | None:
                if name not in counter_curr or name not in prev_counters:
                    return None
                return max(counter_curr[name] - prev_counters[name], 0)

            throttled_usec = counter_delta("cpu_stat_throttled_usec")
            nr_throttled = counter_delta("cpu_stat_nr_throttled")
            if throttled_usec is not None:
                record["cpu_throttled_usec_delta"] = throttled_usec
                record["cpu_throttled_pct"] = round(100.0 * throttled_usec / dt_usec, 3) if dt_usec > 0 else 0.0
            if nr_throttled is not None:
                record["cpu_nr_throttled_delta"] = nr_throttled

            for resource in ("cpu", "memory", "io"):
                for pressure_class in ("some", "full"):
                    delta = counter_delta(f"{resource}_psi_{pressure_class}_total_usec")
                    if delta is not None:
                        record[f"{resource}_psi_{pressure_class}_pct"] = (
                            round(100.0 * delta / dt_usec, 3) if dt_usec > 0 else 0.0
                        )

            for event in ("high", "max", "oom", "oom_kill"):
                delta = counter_delta(f"memory_events_{event}")
                if delta is not None:
                    record[f"memory_{event}_events_delta"] = delta
        return record


def _meminfo_kb() -> dict[str, int]:
    mem: dict[str, int] = {}
    with open("/proc/meminfo") as f:
        for line in f:
            parts = line.split(":")
            if len(parts) == 2:
                key = parts[0].strip()
                val = parts[1].strip().split()[0]
                mem[key] = int(val)
    return mem


def read_meminfo() -> int:
    """Used RAM in KB (MemTotal - MemAvailable)."""
    mem = _meminfo_kb()
    return mem.get("MemTotal", 0) - mem.get("MemAvailable", 0)


def get_cpu_cores() -> int:
    try:
        with open("/proc/cpuinfo") as f:
            return sum(1 for line in f if line.strip().startswith("processor"))
    except OSError:
        return 1


def get_ram_total_gb() -> float:
    return _meminfo_kb().get("MemTotal", 0) / (1024 * 1024)


def _parse_diskstats(text: str, root_block_devices: set[str]) -> tuple[int, int, int, int, int, int]:
    """Aggregate read/write sectors and queue/busy counters for root block devices."""
    read_sectors = 0
    write_sectors = 0
    io_in_progress = 0
    io_time_ms = 0
    weighted_io_time_ms = 0
    device_count = 0
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 14:
            continue
        try:
            name = parts[2]
            # Keep only root block devices to avoid double-counting partitions.
            if root_block_devices and name not in root_block_devices:
                continue
            # Skip virtual devices that distort the host-level picture.
            if name.startswith(("dm-", "loop", "ram")):
                continue
            device_count += 1
            # /proc/diskstats fields 6/10 are sectors read/written, 12 is
            # in-flight I/O, 13/14 are device busy and weighted busy ms.
            read_sectors += int(parts[5])
            write_sectors += int(parts[9])
            io_in_progress += int(parts[11])
            io_time_ms += int(parts[12])
            weighted_io_time_ms += int(parts[13])
        except (ValueError, IndexError):
            continue
    return read_sectors, write_sectors, io_in_progress, io_time_ms, weighted_io_time_ms, device_count


def read_diskstats() -> tuple[int, int, int, int, int, int]:
    """Read cumulative disk counters for root block devices from /proc/diskstats."""
    try:
        root_block_devices = {p.name for p in Path("/sys/block").iterdir() if p.is_dir()}
    except OSError:
        root_block_devices = set()
    with open("/proc/diskstats") as f:
        return _parse_diskstats(f.read(), root_block_devices)


def run_monitor(
    output_jsonl: Path,
    ram_usage_txt: Path | None,
    interval_sec: float,
    stop_file: Path | None,
) -> None:
    """Main monitoring loop."""
    prev_stat = read_proc_stat()
    prev_disk = read_diskstats()
    prev_pressure = read_host_pressure()
    seed_snapshot = read_process_snapshot()
    prev_pid_cpu, prev_pid_io = seed_ya_counters(seed_snapshot, find_ya_process_tree(seed_snapshot))
    test_sampler = TestCgroupSampler()
    prev_ts = time.time()
    cpu_cores = get_cpu_cores()
    ram_total_gb = round(get_ram_total_gb(), 2)
    output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    output_jsonl.write_text("", encoding="utf-8")
    time.sleep(interval_sec)

    with open(output_jsonl, "a") as jf:
        while True:
            if stop_file and stop_file.exists():
                break
            ts = time.time()
            ts_us = int(ts * 1_000_000)
            dt = ts - prev_ts
            prev_ts = ts
            if dt <= 0:
                dt = interval_sec if interval_sec > 0 else 1.0

            # CPU total (absolute)
            curr_stat = read_proc_stat()
            total_delta = curr_stat[0] - prev_stat[0]
            idle_delta = curr_stat[1] - prev_stat[1]
            iowait_delta = curr_stat[2] - prev_stat[2]
            cpu_total_pct = 100.0 * (1 - idle_delta / total_delta) if total_delta > 0 else 0.0
            cpu_iowait_pct = 100.0 * max(0.0, iowait_delta) / total_delta if total_delta > 0 else 0.0
            prev_stat = curr_stat
            curr_pressure = read_host_pressure()
            pressure_pct = pressure_interval_pct(curr_pressure, prev_pressure, dt)
            prev_pressure = curr_pressure

            # ya make process tree
            process_snapshot = read_process_snapshot()
            ya_pids = find_ya_process_tree(process_snapshot)
            pid_data = list(process_snapshot.values())

            cpu_ya_jiffies = 0
            ram_ya_kb = 0
            ya_read_delta = 0
            ya_write_delta = 0
            next_pid_cpu: dict[tuple[int, int], tuple[int, int]] = {}
            next_pid_io: dict[tuple[int, int], tuple[int, int] | None] = {}

            for p in pid_data:
                pid = p["pid"]
                if pid not in ya_pids:
                    continue
                ram_ya_kb += p.get("rss_kb", 0)
                ident = process_identity(p)
                next_pid_cpu[ident] = (p["utime"], p["stime"])
                delta_r, delta_w = apply_io_sample(ident, get_process_io(pid), prev_pid_io, next_pid_io)
                ya_read_delta += delta_r
                ya_write_delta += delta_w
                delta_total = cpu_delta_jiffies(prev_pid_cpu.get(ident), p["utime"], p["stime"])
                if delta_total > 0:
                    cpu_ya_jiffies += delta_total
            prev_pid_cpu = next_pid_cpu
            prev_pid_io = next_pid_io
            cpu_ya_pct = 100.0 * cpu_ya_jiffies / total_delta if total_delta > 0 else 0.0

            disk_ya_read_mb = ya_read_delta / (1024 * 1024)
            disk_ya_write_mb = ya_write_delta / (1024 * 1024)
            disk_ya_read_mbps = disk_ya_read_mb / dt
            disk_ya_write_mbps = disk_ya_write_mb / dt

            # RAM (absolute)
            ram_used_kb = read_meminfo()

            # Disk I/O (absolute)
            curr_disk = read_diskstats()
            disk_read_delta = curr_disk[0] - prev_disk[0]
            disk_write_delta = curr_disk[1] - prev_disk[1]
            disk_read_mb = disk_read_delta * 512 / (1024 * 1024)
            disk_write_mb = disk_write_delta * 512 / (1024 * 1024)
            disk_read_mbps = disk_read_mb / dt
            disk_write_mbps = disk_write_mb / dt
            disk_io_time_ms_delta = max(0, curr_disk[3] - prev_disk[3])
            disk_weighted_io_time_ms_delta = max(0, curr_disk[4] - prev_disk[4])
            prev_disk = curr_disk
            disk_devices_count = curr_disk[5]
            disk_busy_pct = (
                100.0 * disk_io_time_ms_delta / (dt * 1000.0 * disk_devices_count)
                if dt > 0 and disk_devices_count > 0
                else 0.0
            )

            record = {
                "ts": round(ts, 3),
                "ts_us": ts_us,
                "dt_sec": round(dt, 6),
                "cpu_total_pct": round(cpu_total_pct, 2),
                "cpu_iowait_pct": round(cpu_iowait_pct, 2),
                **pressure_pct,
                "cpu_ya_pct": round(cpu_ya_pct, 2),
                "ram_used_kb": ram_used_kb,
                "ram_ya_kb": ram_ya_kb,
                "disk_read_sectors": curr_disk[0],
                "disk_write_sectors": curr_disk[1],
                "disk_read_mb_delta": round(disk_read_mb, 2),
                "disk_write_mb_delta": round(disk_write_mb, 2),
                "disk_read_mbps": round(disk_read_mbps, 2),
                "disk_write_mbps": round(disk_write_mbps, 2),
                "disk_io_in_progress": curr_disk[2],
                "disk_io_time_ms_delta": disk_io_time_ms_delta,
                "disk_weighted_io_time_ms_delta": disk_weighted_io_time_ms_delta,
                "disk_devices_count": disk_devices_count,
                "disk_busy_pct": round(disk_busy_pct, 2),
                "disk_ya_read_mb_delta": round(disk_ya_read_mb, 2),
                "disk_ya_write_mb_delta": round(disk_ya_write_mb, 2),
                "disk_ya_read_mbps": round(disk_ya_read_mbps, 2),
                "disk_ya_write_mbps": round(disk_ya_write_mbps, 2),
            }
            record["cpu_cores"] = cpu_cores
            record["ram_total_gb"] = ram_total_gb
            record["kind"] = "host"
            jf.write(json.dumps(record) + "\n")
            for test_rec in test_sampler.sample(dt, ts, ts_us, process_snapshot):
                jf.write(json.dumps(test_rec) + "\n")
            jf.flush()

            if ram_usage_txt:
                with open(ram_usage_txt, "a") as rf:
                    rf.write(f"{int(ts)} {ram_used_kb}\n")

            time.sleep(interval_sec)


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor CPU, RAM, disk during ya make")
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output JSONL file path",
    )
    parser.add_argument(
        "--ram-usage-file",
        type=Path,
        default=None,
        help="Also append to ram_usage.txt (format: ts used_kb) for report_ram_analyzer",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=3.0,
        help="Sampling interval in seconds (default: 3)",
    )
    parser.add_argument(
        "--stop-file",
        type=Path,
        default=None,
        help="Stop monitoring when this file appears",
    )
    args = parser.parse_args()

    if args.ram_usage_file and args.ram_usage_file.exists():
        args.ram_usage_file.unlink()

    try:
        run_monitor(
            output_jsonl=args.output,
            ram_usage_txt=args.ram_usage_file,
            interval_sec=args.interval,
            stop_file=args.stop_file,
        )
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
