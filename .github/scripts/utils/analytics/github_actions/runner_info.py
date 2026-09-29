#!/usr/bin/env python3
"""Runner inventory (cached) and a small CPU/RAM/disk usage snapshot."""

from __future__ import annotations

import json
import os
import shutil
from typing import Any, Callable, Dict, Optional, Tuple

INVENTORY_LABEL = "runner.inventory"
USAGE_LABEL = "runner.usage"
CACHE_ENV = "CI_RUNNER_INFO_FILE"
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


def collect_usage() -> Dict[str, Any]:
    try:
        return _collect_usage()
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
    cpu_count = os.cpu_count()
    if cpu_count:
        out["cpu_count"] = int(cpu_count)
    mem = _meminfo()
    total = mem.get("MemTotal")
    if total is not None:
        out["mem_total_bytes"] = total
    root = _disk_usage("/")
    if root is not None:
        out["disk_total_bytes"] = root["total_bytes"]
    return out


def _collect_usage() -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    cpu_pct = _cpu_load_pct()
    if cpu_pct is not None:
        out["cpu_pct"] = cpu_pct
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
    return out


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
        free = out.get("MemFree", 0) + out.get("Buffers", 0) + out.get("Cached", 0)
        out["MemAvailable"] = min(free, out["MemTotal"])
    return out


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


def _cpu_load_pct() -> Optional[float]:
    try:
        with open("/proc/loadavg", encoding="utf-8") as handle:
            load1 = float(handle.read().split()[0])
        cpus = os.cpu_count() or 1
        return round(100.0 * load1 / cpus, 2)
    except (OSError, ValueError, IndexError):
        try:
            load1 = os.getloadavg()[0]
        except OSError:
            return None
        cpus = os.cpu_count() or 1
        return round(100.0 * load1 / cpus, 2)
