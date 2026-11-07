"""Measured host capacity for the tests resource dashboard.

Chart limit lines and mem→cpu recommendations use /proc data from
``resources_monitor.jsonl``. If that data is missing, limits are omitted.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

# Tuning applied to measured MemTotal / CPU (same ratios as ya make MemoryMax).
MEM_BUDGET_FRACTION = 0.70
YA_MAKE_MEM_FRACTION = 0.95
MEM_MIN_CHUNK_GB = 24.0
CPU_SATURATION_PCT = 90.0
NEAR_BUDGET_FACTOR = 1.10


@dataclass(frozen=True)
class RunnerCapacity:
    vcpu: int
    ram_gb: float
    mem_budget_gb: float
    mem_min_chunk_gb: float
    cpu_saturation_pct: float
    near_budget_factor: float
    mem_cpu_tiers: tuple[int, ...]
    ya_make_mem_limit_gb: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "vcpu": self.vcpu,
            "ram_gb": round(self.ram_gb, 1),
            "mem_budget_gb": round(self.mem_budget_gb, 1),
            "mem_min_chunk_gb": self.mem_min_chunk_gb,
            "cpu_saturation_pct": self.cpu_saturation_pct,
            "near_budget_factor": self.near_budget_factor,
            "ya_make_mem_limit_gb": round(self.ya_make_mem_limit_gb, 1),
        }


def mem_cpu_tiers_for(max_cores: int) -> tuple[int, ...]:
    base = (1, 2, 4, 8, 16, 32, 48, 64, 96, 128)
    tiers = [t for t in base if t <= max_cores]
    if max_cores > 0 and (not tiers or tiers[-1] != max_cores):
        tiers.append(max_cores)
    return tuple(tiers)


def capacity_from_measured(
    *,
    cpu_cores: Optional[float],
    ram_gb: Optional[float],
) -> Optional[RunnerCapacity]:
    """Build capacity from measured host specs. None if either value is missing."""
    try:
        cpu = float(cpu_cores) if cpu_cores is not None else 0.0
    except (TypeError, ValueError):
        cpu = 0.0
    try:
        ram = float(ram_gb) if ram_gb is not None else 0.0
    except (TypeError, ValueError):
        ram = 0.0
    if cpu <= 0 or ram <= 0:
        return None

    vcpu = max(1, int(round(cpu)))
    return RunnerCapacity(
        vcpu=vcpu,
        ram_gb=ram,
        mem_budget_gb=ram * MEM_BUDGET_FRACTION,
        mem_min_chunk_gb=MEM_MIN_CHUNK_GB,
        cpu_saturation_pct=CPU_SATURATION_PCT,
        near_budget_factor=NEAR_BUDGET_FACTOR,
        mem_cpu_tiers=mem_cpu_tiers_for(vcpu),
        ya_make_mem_limit_gb=ram * YA_MAKE_MEM_FRACTION,
    )


def _first_positive(records: Optional[list[dict[str, Any]]], key: str) -> Optional[float]:
    for rec in records or []:
        raw = rec.get(key)
        if raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return None


def extract_measured_host_specs(
    overlay: Optional[dict[str, Any]],
    *,
    records: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Optional[float]]:
    measured_cpu = _first_positive(records, "cpu_cores")
    if measured_cpu is None and overlay:
        raw_cpu = overlay.get("cpu_cores")
        try:
            measured_cpu = float(raw_cpu) if raw_cpu else None
        except (TypeError, ValueError):
            measured_cpu = None
        if measured_cpu is not None and measured_cpu <= 0:
            measured_cpu = None

    measured_ram = _first_positive(records, "ram_total_gb")
    if measured_ram is None and overlay:
        raw_ram = overlay.get("ram_total_gb")
        try:
            measured_ram = float(raw_ram) if raw_ram else None
        except (TypeError, ValueError):
            measured_ram = None
        if measured_ram is not None and measured_ram <= 0:
            measured_ram = None

    return {"cpu_cores": measured_cpu, "ram_gb": measured_ram}


def enrich_resources_overlay(
    overlay: Optional[dict[str, Any]],
    *,
    records: Optional[list[dict[str, Any]]] = None,
) -> tuple[Optional[dict[str, Any]], Optional[RunnerCapacity]]:
    """Attach measured host limits. Omit limits when monitor data is incomplete."""
    if overlay is None:
        return None, None

    out = dict(overlay)
    measured = extract_measured_host_specs(out, records=records)
    out["measured"] = measured

    capacity = capacity_from_measured(
        cpu_cores=measured.get("cpu_cores"),
        ram_gb=measured.get("ram_gb"),
    )
    if capacity is None:
        out.pop("runner_limits", None)
        out.pop("runner_capacity", None)
        return out, None

    out["runner_limits"] = {
        "cpu_cores_max": capacity.vcpu,
        "ram_gb_max": capacity.ram_gb,
        "mem_budget_gb": round(capacity.mem_budget_gb, 1),
        "ya_make_mem_limit_gb": round(capacity.ya_make_mem_limit_gb, 1),
    }
    out["runner_capacity"] = capacity.to_dict()
    return out, capacity
