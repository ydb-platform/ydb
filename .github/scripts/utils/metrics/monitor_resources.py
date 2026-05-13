#!/usr/bin/env python3
"""
Monitor system resources (CPU, RAM, disk I/O) during ya make execution.

Logs metrics every N seconds to JSONL file. Data can be correlated with:
  - report.json (suite_start_timestamp, wall_time)
  - ya_evlog.jsonl / Chromium trace timeline

Output format (JSONL, one JSON object per line):
  - ts, ts_us: Unix timestamp
  - cpu_total_pct, ram_used_kb: system-wide CPU/RAM usage
  - disk_*_sectors: ABSOLUTE system-wide disk I/O counters (cumulative)
  - disk_*_mb_delta (e.g. disk_read_mb_delta): per-sample delta in MB
  - disk_*_mbps (e.g. disk_read_mbps): normalized MB/s by actual sample interval
  - cpu_ya_pct, ram_ya_kb, disk_ya_*: ya make process tree only
  - cpu_per_pid: ALL processes in ya tree (no top-N limit)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path


def read_proc_stat() -> tuple[float, float]:
    """Read /proc/stat, return (total_jiffies, idle_jiffies)."""
    with open("/proc/stat") as f:
        line = f.readline()
    parts = line.split()
    # cpu user nice system idle iowait irq softirq steal guest guest_nice
    total = sum(int(x) for x in parts[1:])
    idle = int(parts[4]) if len(parts) > 4 else 0
    return float(total), float(idle)


def read_proc_uptime() -> float:
    """Read system uptime in seconds."""
    with open("/proc/uptime") as f:
        return float(f.read().split()[0])


def _read_cmdline(pid: int) -> str:
    try:
        return (Path("/proc") / str(pid) / "cmdline").read_text().replace("\x00", " ")
    except (OSError, ValueError):
        return ""


def _is_ya_root(cmdline: str, comm: str) -> bool:
    """Process is a ya root (ya, ya-tc, ya.make). Match by executable basename or comm."""
    ya_names = ("ya", "ya-tc", "ya.make")
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
        if p.endswith("/ya") or p.endswith("/ya-tc") or p.endswith("/ya.make"):
            return True
    return False


def find_ya_process_tree() -> set[int]:
    """Find all PIDs in ya make process tree (roots + descendants)."""
    proc = Path("/proc")
    ppid_map: dict[int, int] = {}
    cmdline_map: dict[int, str] = {}
    comm_map: dict[int, str] = {}

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
            if len(rest) < 14:
                continue
            ppid = int(rest[1])
            ppid_map[pid] = ppid
            comm_map[pid] = comm
            cmdline_map[pid] = _read_cmdline(pid)
        except (OSError, ValueError):
            continue

    roots = {p for p, cmd in cmdline_map.items() if _is_ya_root(cmd, comm_map.get(p, ""))}
    ya_pids: set[int] = set(roots)
    # Include ancestors (e.g. main "ya" is parent of "ya-tc" workers)
    for r in roots:
        pid = ppid_map.get(r)
        while pid and pid != 1:
            ya_pids.add(pid)
            pid = ppid_map.get(pid)
    # Include descendants of roots
    changed = True
    while changed:
        changed = False
        for pid, ppid in ppid_map.items():
            if pid not in ya_pids and ppid in ya_pids:
                ya_pids.add(pid)
                changed = True
    return ya_pids


def get_process_stats(pid: int) -> dict | None:
    """Get utime, stime, rss_kb for a process. Returns None on error."""
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_text()
        rparen = stat.rfind(")")
        if rparen < 0:
            return None
        rest = stat[rparen + 2 :].split()
        if len(rest) < 24:
            return None
        utime = int(rest[11])
        stime = int(rest[12])
        rss_pages = int(rest[21])
        comm = stat[stat.find("(") + 1 : rparen][:80]
        try:
            page_size_kb = os.sysconf("SC_PAGE_SIZE") // 1024
        except (OSError, ValueError):
            page_size_kb = 4
        if page_size_kb <= 0:
            page_size_kb = 4
        return {
            "pid": pid,
            "comm": comm,
            "utime": utime,
            "stime": stime,
            "rss_kb": rss_pages * page_size_kb,
        }
    except (OSError, ValueError):
        return None


def get_process_io(pid: int) -> tuple[int, int]:
    """Get read_bytes, write_bytes from /proc/pid/io. Returns (0,0) if unreadable."""
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
        return 0, 0


def get_cpu_per_process() -> list[dict]:
    """Get CPU usage per process from /proc/*/stat."""
    result = []
    proc = Path("/proc")
    for pid_dir in proc.iterdir():
        if not pid_dir.is_dir() or not pid_dir.name.isdigit():
            continue
        pid = int(pid_dir.name)
        s = get_process_stats(pid)
        if s:
            result.append(s)
    return result


def read_meminfo() -> int:
    """Return used RAM in KB (MemTotal - MemAvailable)."""
    mem = {}
    with open("/proc/meminfo") as f:
        for line in f:
            parts = line.split(":")
            if len(parts) == 2:
                key = parts[0].strip()
                val = parts[1].strip().split()[0]
                mem[key] = int(val)
    total = mem.get("MemTotal", 0)
    avail = mem.get("MemAvailable", 0)
    return total - avail


def get_cpu_cores() -> int:
    """Return number of CPU cores from /proc/cpuinfo."""
    try:
        with open("/proc/cpuinfo") as f:
            return sum(1 for line in f if line.strip().startswith("processor"))
    except OSError:
        return 1


def get_ram_total_gb() -> float:
    """Return total RAM in GB from /proc/meminfo."""
    mem = {}
    with open("/proc/meminfo") as f:
        for line in f:
            parts = line.split(":")
            if len(parts) == 2:
                key = parts[0].strip()
                val = parts[1].strip().split()[0]
                mem[key] = int(val)
    return mem.get("MemTotal", 0) / (1024 * 1024)


def read_diskstats() -> tuple[int, int]:
    """Read /proc/diskstats. Return (total_read_sectors, total_write_sectors) for root block devices."""
    read_sectors = 0
    write_sectors = 0
    try:
        root_block_devices = {p.name for p in Path("/sys/block").iterdir() if p.is_dir()}
    except OSError:
        root_block_devices = set()
    with open("/proc/diskstats") as f:
        for line in f:
            parts = line.split()
            if len(parts) < 14:
                continue
            try:
                name = parts[2]
                # Keep only root block devices from /sys/block.
                # This avoids double-counting partitions (e.g. nvme0n1 + nvme0n1p1).
                if root_block_devices and name not in root_block_devices:
                    continue
                # Skip virtual devices that distort host-level picture.
                if name.startswith("dm-") or name.startswith("loop") or name.startswith("ram"):
                    continue
                # columns 4-5: read sectors, 8-9: write sectors (0-indexed)
                read_sectors += int(parts[5])
                write_sectors += int(parts[9])
            except (ValueError, IndexError):
                continue
    return read_sectors, write_sectors


def run_monitor(
    output_jsonl: Path,
    ram_usage_txt: Path | None,
    interval_sec: float,
    stop_file: Path | None,
) -> None:
    """Main monitoring loop."""
    prev_stat = read_proc_stat()
    prev_disk = read_diskstats()
    prev_pid_cpu: dict[int, tuple[int, int]] = {}
    prev_ya_io: tuple[int, int] = (0, 0)
    meta_added = False
    time.sleep(interval_sec)
    prev_ts = time.time()

    with open(output_jsonl, "w") as jf:
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
            cpu_total_pct = 100.0 * (1 - idle_delta / total_delta) if total_delta > 0 else 0.0
            prev_stat = curr_stat

            # ya make process tree
            ya_pids = find_ya_process_tree()
            pid_data = get_cpu_per_process()

            # CPU per process + ya aggregates (ALL ya tree, no top-N limit)
            cpu_per_pid: list[dict] = []
            cpu_ya_jiffies = 0
            ram_ya_kb = 0
            ya_read_bytes = 0
            ya_write_bytes = 0

            for p in pid_data:
                pid = p["pid"]
                prev_val = prev_pid_cpu.get(pid, (0, 0))
                delta_total = (p["utime"] - prev_val[0]) + (p["stime"] - prev_val[1])
                cpu_pct = 100.0 * delta_total / total_delta if total_delta > 0 and delta_total > 0 else 0.0

                if pid in ya_pids:
                    prev_pid_cpu[pid] = (p["utime"], p["stime"])
                    cpu_ya_jiffies += delta_total
                    ram_ya_kb += p.get("rss_kb", 0)
                    r, w = get_process_io(pid)
                    ya_read_bytes += r
                    ya_write_bytes += w
                    cpu_per_pid.append({
                        "pid": pid,
                        "comm": p["comm"],
                        "cpu_pct": round(cpu_pct, 2),
                        "utime": p["utime"],
                        "stime": p["stime"],
                    })
            prev_pid_cpu = {k: v for k, v in prev_pid_cpu.items() if k in ya_pids}
            cpu_per_pid.sort(key=lambda x: x["cpu_pct"], reverse=True)
            cpu_ya_pct = 100.0 * cpu_ya_jiffies / total_delta if total_delta > 0 else 0.0

            # ya disk delta
            disk_ya_read_mb = (ya_read_bytes - prev_ya_io[0]) / (1024 * 1024)
            disk_ya_write_mb = (ya_write_bytes - prev_ya_io[1]) / (1024 * 1024)
            disk_ya_read_mbps = disk_ya_read_mb / dt
            disk_ya_write_mbps = disk_ya_write_mb / dt
            prev_ya_io = (ya_read_bytes, ya_write_bytes)

            # RAM (absolute)
            ram_used_kb = read_meminfo()

            # Disk I/O (absolute)
            curr_disk = read_diskstats()
            disk_read_delta = curr_disk[0] - prev_disk[0]
            disk_write_delta = curr_disk[1] - prev_disk[1]
            prev_disk = curr_disk
            disk_read_mb = disk_read_delta * 512 / (1024 * 1024)
            disk_write_mb = disk_write_delta * 512 / (1024 * 1024)
            disk_read_mbps = disk_read_mb / dt
            disk_write_mbps = disk_write_mb / dt

            record = {
                "ts": round(ts, 3),
                "ts_us": ts_us,
                "dt_sec": round(dt, 6),
                "cpu_total_pct": round(cpu_total_pct, 2),
                "cpu_ya_pct": round(cpu_ya_pct, 2),
                "cpu_per_pid": cpu_per_pid,
                "ram_used_kb": ram_used_kb,
                "ram_ya_kb": ram_ya_kb,
                "disk_read_sectors": curr_disk[0],
                "disk_write_sectors": curr_disk[1],
                "disk_read_mb_delta": round(disk_read_mb, 2),
                "disk_write_mb_delta": round(disk_write_mb, 2),
                "disk_read_mbps": round(disk_read_mbps, 2),
                "disk_write_mbps": round(disk_write_mbps, 2),
                "disk_ya_read_mb_delta": round(disk_ya_read_mb, 2),
                "disk_ya_write_mb_delta": round(disk_ya_write_mb, 2),
                "disk_ya_read_mbps": round(disk_ya_read_mbps, 2),
                "disk_ya_write_mbps": round(disk_ya_write_mbps, 2),
            }
            if not meta_added:
                record["cpu_cores"] = get_cpu_cores()
                record["ram_total_gb"] = round(get_ram_total_gb(), 2)
                meta_added = True
            jf.write(json.dumps(record) + "\n")
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
