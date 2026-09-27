#!/usr/bin/env python3
"""Runner inventory (cached) and per-event CPU/RAM/disk usage snapshots."""

from __future__ import annotations

import json
import os
import shutil
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

INVENTORY_LABEL = "runner.inventory"
USAGE_LABEL = "runner.usage"
CACHE_ENV = "CI_RUNNER_INFO_FILE"
USAGE_CPU_SAMPLE_SEC = 0.05
MAX_DISKS = 16
SKIP_DISK_PREFIXES = ("loop", "ram", "dm-", "sr", "fd")
REAL_FS_TYPES = frozenset(
    {
        "ext2",
        "ext3",
        "ext4",
        "xfs",
        "btrfs",
        "overlay",
        "overlay2",
        "erofs",
        "virtiofs",
        "9p",
        "nfs",
        "nfs4",
        "zfs",
    }
)
TRUTHY = frozenset({"1", "true", "yes", "on"})


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in TRUTHY
    return False


def pop_runner_options(data: Optional[Dict[str, Any]]) -> Tuple[bool, bool]:
    if not data:
        return False, False
    return as_bool(data.pop("runner", False)), as_bool(data.pop("usage", False))


def runner_cache_path(metrics_path: Optional[str] = None) -> str:
    env_path = os.environ.get(CACHE_ENV)
    if env_path:
        return env_path
    runner_temp = os.environ.get("RUNNER_TEMP")
    if runner_temp:
        return os.path.join(runner_temp, "ci_runner_info.json")
    base = metrics_path or os.environ.get("CI_METRICS_FILE") or "ci_metrics.jsonl"
    return f"{base}.runner.json"


def _read_cache(path: str) -> Dict[str, Any]:
    if not path or not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) and payload else {}


def _write_cache(path: str, inventory: Dict[str, Any]) -> None:
    if not path or not inventory:
        return
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(inventory, handle, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)


def collect_inventory() -> Dict[str, Any]:
    try:
        return _collect_inventory()
    except Exception:  # noqa: BLE001 — telemetry must not fail CI
        return {}


def collect_usage(*, sample_sec: float = USAGE_CPU_SAMPLE_SEC) -> Dict[str, Any]:
    try:
        return _collect_usage(sample_sec=sample_sec)
    except Exception:  # noqa: BLE001 — telemetry must not fail CI
        return {}


def load_or_collect_inventory(
    metrics_path: Optional[str] = None,
    *,
    collect: Optional[Callable[[], Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    path = runner_cache_path(metrics_path)
    cached = _read_cache(path)
    if cached:
        return cached
    inventory = (collect or collect_inventory)()
    if inventory:
        _write_cache(path, inventory)
    return inventory


def apply_runner_labels(
    labels: Dict[str, Any],
    *,
    runner: bool = False,
    usage: bool = False,
    metrics_path: Optional[str] = None,
    collect_inventory_fn: Optional[Callable[[], Dict[str, Any]]] = None,
    collect_usage_fn: Optional[Callable[[], Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    if runner:
        inventory = load_or_collect_inventory(metrics_path, collect=collect_inventory_fn)
        if inventory:
            labels.setdefault(INVENTORY_LABEL, inventory)
    if usage:
        snapshot = (collect_usage_fn or collect_usage)()
        if snapshot:
            labels[USAGE_LABEL] = snapshot
    return labels


def _collect_inventory() -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    boot_time = _boot_time()
    if boot_time is not None:
        out["boot_time"] = boot_time
    cpu_count = _cpu_count()
    if cpu_count is not None:
        out["cpu_count"] = cpu_count
    cpu_model = _cpu_model()
    if cpu_model:
        out["cpu_model"] = cpu_model
    mem = _meminfo()
    total = mem.get("MemTotal")
    if total is not None:
        out["mem_total_bytes"] = total
    disks = _physical_disks()
    if disks:
        out["disks"] = disks
        out["disk_total_bytes"] = sum(int(item.get("size_bytes") or 0) for item in disks)
    else:
        root = _disk_usage("/")
        if root is not None:
            out["disk_total_bytes"] = root["total_bytes"]
            out["disks"] = [{"name": "/", "size_bytes": root["total_bytes"]}]
    return out


def _collect_usage(*, sample_sec: float) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    cpu_pct = _cpu_pct(sample_sec)
    if cpu_pct is not None:
        out["cpu_pct"] = cpu_pct
    loadavg = _loadavg()
    if loadavg:
        out["loadavg_1"] = loadavg[0]
        out["loadavg_5"] = loadavg[1]
        out["loadavg_15"] = loadavg[2]
    mem = _meminfo()
    total = mem.get("MemTotal")
    available = mem.get("MemAvailable")
    if available is not None:
        out["mem_available_bytes"] = available
    if total is not None and available is not None:
        out["mem_used_bytes"] = max(total - available, 0)
    root = _disk_usage("/")
    if root is not None:
        out["disk_used_bytes"] = root["used_bytes"]
        out["disk_free_bytes"] = root["free_bytes"]
        out["disk_total_bytes"] = root["total_bytes"]
    mounts = _mount_usage()
    if mounts:
        out["disks"] = mounts
    return out


def _boot_time() -> Optional[int]:
    try:
        with open("/proc/stat", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("btime "):
                    return int(line.split()[1])
    except (OSError, ValueError, IndexError):
        pass
    try:
        with open("/proc/uptime", encoding="utf-8") as handle:
            uptime = float(handle.read().split()[0])
        return int(time.time() - uptime)
    except (OSError, ValueError, IndexError):
        return None


def _cpu_count() -> Optional[int]:
    count = os.cpu_count()
    if count:
        return int(count)
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as handle:
            n = sum(1 for line in handle if line.startswith("processor"))
    except OSError:
        return None
    return n or None


def _cpu_model() -> Optional[str]:
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as handle:
            hardware = None
            model = None
            for line in handle:
                if ":" not in line:
                    continue
                key, value = line.split(":", 1)
                key = key.strip().lower()
                value = value.strip()
                if not value:
                    continue
                if key == "model name":
                    return value[:256]
                if key == "hardware" and hardware is None:
                    hardware = value
                if key == "model" and model is None:
                    model = value
    except OSError:
        return None
    text = hardware or model
    return text[:256] if text else None


def _meminfo() -> Dict[str, int]:
    out: Dict[str, int] = {}
    try:
        with open("/proc/meminfo", encoding="utf-8") as handle:
            lines = handle.readlines()
    except OSError:
        return out
    wanted = {"MemTotal", "MemAvailable", "MemFree", "Buffers", "Cached"}
    for line in lines:
        if ":" not in line:
            continue
        key, rest = line.split(":", 1)
        key = key.strip()
        if key not in wanted:
            continue
        try:
            kib = int(rest.strip().split()[0])
        except (ValueError, IndexError):
            continue
        out[key] = kib * 1024
    if "MemAvailable" not in out and "MemTotal" in out:
        used_free = out.get("MemFree", 0) + out.get("Buffers", 0) + out.get("Cached", 0)
        out["MemAvailable"] = max(out["MemTotal"] - used_free, 0)
    return out


def _physical_disks() -> List[Dict[str, Any]]:
    disks: List[Dict[str, Any]] = []
    sys_block = "/sys/block"
    try:
        names = sorted(os.listdir(sys_block))
    except OSError:
        return disks
    for name in names:
        if name.startswith(SKIP_DISK_PREFIXES):
            continue
        size_path = os.path.join(sys_block, name, "size")
        try:
            with open(size_path, encoding="utf-8") as handle:
                sectors = int(handle.read().strip() or "0")
        except (OSError, ValueError):
            continue
        size_bytes = sectors * 512
        if size_bytes <= 0:
            continue
        disks.append({"name": name, "size_bytes": size_bytes})
        if len(disks) >= MAX_DISKS:
            break
    return disks


def _disk_usage(path: str) -> Optional[Dict[str, int]]:
    try:
        usage = shutil.disk_usage(path)
    except OSError:
        return None
    return {
        "total_bytes": int(usage.total),
        "used_bytes": int(usage.used),
        "free_bytes": int(usage.free),
    }


def _mount_usage() -> List[Dict[str, Any]]:
    mounts: List[Dict[str, Any]] = []
    seen = set()
    try:
        with open("/proc/mounts", encoding="utf-8") as handle:
            lines = handle.readlines()
    except OSError:
        lines = []
    candidates = [("/", None)]
    for line in lines:
        parts = line.split()
        if len(parts) < 3:
            continue
        device, mountpoint, fstype = parts[0], parts[1], parts[2]
        if fstype not in REAL_FS_TYPES:
            continue
        mountpoint = mountpoint.replace("\\040", " ")
        candidates.append((mountpoint, device))
    for mountpoint, device in candidates:
        key = device or mountpoint
        if key in seen:
            continue
        usage = _disk_usage(mountpoint)
        if usage is None:
            continue
        seen.add(key)
        mounts.append(
            {
                "mount": mountpoint,
                "total_bytes": usage["total_bytes"],
                "used_bytes": usage["used_bytes"],
                "free_bytes": usage["free_bytes"],
            }
        )
        if len(mounts) >= MAX_DISKS:
            break
    return mounts


def _read_proc_stat() -> Optional[Tuple[float, float]]:
    try:
        with open("/proc/stat", encoding="utf-8") as handle:
            parts = handle.readline().split()
        total = sum(int(item) for item in parts[1:])
        idle = int(parts[4]) if len(parts) > 4 else 0
        if len(parts) > 5:
            idle += int(parts[5])
        return float(total), float(idle)
    except (OSError, ValueError, IndexError):
        return None


def _cpu_pct(sample_sec: float) -> Optional[float]:
    first = _read_proc_stat()
    if first is None:
        return None
    if sample_sec > 0:
        time.sleep(sample_sec)
    second = _read_proc_stat()
    if second is None:
        return None
    total_delta = second[0] - first[0]
    idle_delta = second[1] - first[1]
    if total_delta <= 0:
        return 0.0
    return round(100.0 * (1.0 - idle_delta / total_delta), 2)


def _loadavg() -> Optional[Tuple[float, float, float]]:
    try:
        with open("/proc/loadavg", encoding="utf-8") as handle:
            parts = handle.read().split()
        return float(parts[0]), float(parts[1]), float(parts[2])
    except (OSError, ValueError, IndexError):
        return None
