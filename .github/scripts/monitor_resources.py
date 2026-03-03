#!/usr/bin/env python3
"""
Monitor system resources (CPU, RAM, disk I/O) during ya make execution.

Logs metrics every N seconds to JSONL file. Data can be correlated with:
  - report.json (suite_start_timestamp, wall_time)
  - ya_evlog.jsonl / Chromium trace timeline

Output format (JSONL, one JSON object per line):
  - ts: Unix timestamp (seconds)
  - ts_us: Unix timestamp in microseconds (for trace alignment)
  - cpu_total_pct: system-wide CPU usage %
  - cpu_per_pid: list of {pid, comm, cpu_pct, utime, stime} for top processes
  - ram_used_kb: used RAM (MemTotal - MemAvailable)
  - disk_read_sectors, disk_write_sectors: cumulative from /proc/diskstats
  - disk_read_mb, disk_write_mb: delta since previous sample (computed)
"""

from __future__ import annotations

import argparse
import json
import os
import signal
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


def get_cpu_per_process() -> list[dict]:
    """Get CPU usage per process from /proc/*/stat. Returns top processes by utime+stime."""
    result = []
    proc = Path("/proc")
    for pid_dir in proc.iterdir():
        if not pid_dir.is_dir() or not pid_dir.name.isdigit():
            continue
        pid = int(pid_dir.name)
        try:
            stat_path = pid_dir / "stat"
            stat = stat_path.read_text()
            # Format: pid (comm) state ppid ... utime stime ... starttime
            # comm can contain spaces, so we need to find last ) for state
            rparen = stat.rfind(")")
            if rparen < 0:
                continue
            rest = stat[rparen + 2 :].split()
            if len(rest) < 14:
                continue
            utime = int(rest[11])
            stime = int(rest[12])
            comm = stat[stat.find("(") + 1 : rparen][:80]
            result.append({"pid": pid, "comm": comm, "utime": utime, "stime": stime})
        except (OSError, ValueError):
            continue
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


def read_diskstats() -> tuple[int, int]:
    """Read /proc/diskstats. Return (total_read_sectors, total_write_sectors) for block devices."""
    read_sectors = 0
    write_sectors = 0
    with open("/proc/diskstats") as f:
        for line in f:
            parts = line.split()
            if len(parts) < 14:
                continue
            # Skip partitions (major 1 = ram, 7 = loop, 11 = sr - skip; keep 8, 259 = nvme/sd)
            try:
                major = int(parts[0])
                minor = int(parts[1])
                name = parts[2]
                # Skip dm-*, loop*, ram*
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
    top_pids: int,
    stop_file: Path | None,
) -> None:
    """Main monitoring loop."""
    # Prime: first sample needs a prior read for delta-based metrics
    prev_stat = read_proc_stat()
    prev_uptime = read_proc_uptime()
    prev_disk = read_diskstats()
    prev_pid_cpu: dict[int, tuple[int, int]] = {}
    time.sleep(interval_sec)

    with open(output_jsonl, "w") as jf:
        while True:
            if stop_file and stop_file.exists():
                break
            ts = time.time()
            ts_us = int(ts * 1_000_000)

            # CPU total
            curr_stat = read_proc_stat()
            curr_uptime = read_proc_uptime()
            total_delta = curr_stat[0] - prev_stat[0]
            idle_delta = curr_stat[1] - prev_stat[1]
            cpu_total_pct = 100.0 * (1 - idle_delta / total_delta) if total_delta > 0 else 0.0
            prev_stat = curr_stat

            # CPU per process (delta of utime+stime over interval)
            pid_data = get_cpu_per_process()
            elapsed = curr_uptime - prev_uptime
            cpu_per_pid = []
            for p in pid_data:
                key = (p["pid"], p["utime"], p["stime"])
                prev_val = prev_pid_cpu.get(p["pid"], (0, 0))
                prev_pid_cpu[p["pid"]] = (p["utime"], p["stime"])
                delta_utime = p["utime"] - prev_val[0]
                delta_stime = p["stime"] - prev_val[1]
                delta_total = delta_utime + delta_stime
                # CPU % = (delta jiffies / total jiffies) * 100. Jiffy is typically 10ms.
                cpu_pct = 100.0 * delta_total / total_delta if total_delta > 0 and delta_total > 0 else 0.0
                if cpu_pct > 0.1:  # filter noise
                    cpu_per_pid.append(
                        {
                            "pid": p["pid"],
                            "comm": p["comm"],
                            "cpu_pct": round(cpu_pct, 2),
                            "utime": p["utime"],
                            "stime": p["stime"],
                        }
                    )
            cpu_per_pid.sort(key=lambda x: x["cpu_pct"], reverse=True)
            cpu_per_pid = cpu_per_pid[:top_pids]

            # RAM
            ram_used_kb = read_meminfo()

            # Disk I/O
            curr_disk = read_diskstats()
            disk_read_delta = curr_disk[0] - prev_disk[0]
            disk_write_delta = curr_disk[1] - prev_disk[1]
            prev_disk = curr_disk
            # sector = 512 bytes
            disk_read_mb = disk_read_delta * 512 / (1024 * 1024)
            disk_write_mb = disk_write_delta * 512 / (1024 * 1024)

            record = {
                "ts": round(ts, 3),
                "ts_us": ts_us,
                "cpu_total_pct": round(cpu_total_pct, 2),
                "cpu_per_pid": cpu_per_pid,
                "ram_used_kb": ram_used_kb,
                "disk_read_sectors": curr_disk[0],
                "disk_write_sectors": curr_disk[1],
                "disk_read_mb_delta": round(disk_read_mb, 2),
                "disk_write_mb_delta": round(disk_write_mb, 2),
            }
            jf.write(json.dumps(record) + "\n")
            jf.flush()

            if ram_usage_txt:
                with open(ram_usage_txt, "a") as rf:
                    rf.write(f"{int(ts)} {ram_used_kb}\n")

            prev_uptime = curr_uptime
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
        "--top-pids",
        type=int,
        default=50,
        help="Max number of top CPU processes to log per sample (default: 50)",
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
            top_pids=args.top_pids,
            stop_file=args.stop_file,
        )
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
