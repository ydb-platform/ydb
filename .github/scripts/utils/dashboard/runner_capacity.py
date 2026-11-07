"""Measured host capacity for the tests resource dashboard.

Ceilings and mem→cpu budgets come from resources_monitor.jsonl host fields.
Tuning knobs live in ``.github/config/tests_resource_dashboard.yml`` (required).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

CONFIG_REL_PATH = Path(".github/config/tests_resource_dashboard.yml")
_MEM_CPU_TIERS_BASE = (1, 2, 4, 8, 16, 32, 48, 64, 96, 128)


@dataclass(frozen=True)
class DashboardTuning:
    mem_budget_fraction: float
    ya_make_mem_fraction: float
    mem_min_chunk_gb: float
    cpu_saturation_pct: float
    near_budget_factor: float


@dataclass(frozen=True)
class MeasuredHost:
    vcpu: int
    ram_gb: float

    def mem_budget_gb(self, tuning: DashboardTuning) -> float:
        return self.ram_gb * tuning.mem_budget_fraction

    def ya_make_mem_limit_gb(self, tuning: DashboardTuning) -> float:
        return self.ram_gb * tuning.ya_make_mem_fraction

    def to_host_dict(self, tuning: DashboardTuning) -> dict[str, Any]:
        return {
            "vcpu": self.vcpu,
            "ram_gb": round(self.ram_gb, 1),
            "mem_budget_gb": round(self.mem_budget_gb(tuning), 1),
            "ya_make_mem_limit_gb": round(self.ya_make_mem_limit_gb(tuning), 1),
        }


def _default_config_path() -> Path:
    # runner_capacity.py → dashboard → utils → scripts → .github → repo root
    return Path(__file__).resolve().parents[4] / CONFIG_REL_PATH


def load_dashboard_tuning(config_path: Optional[Path] = None) -> DashboardTuning:
    path = Path(config_path) if config_path else _default_config_path()
    if not path.is_file():
        raise FileNotFoundError(f"Missing dashboard tuning config: {path}")
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError(f"PyYAML is required to load {path}") from exc
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"Dashboard tuning config must be a mapping: {path}")

    def require_float(key: str) -> float:
        if key not in raw:
            raise KeyError(f"Missing {key!r} in {path}")
        try:
            value = float(raw[key])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid {key!r} in {path}: {raw[key]!r}") from exc
        if value <= 0:
            raise ValueError(f"{key!r} must be positive in {path}")
        return value

    return DashboardTuning(
        mem_budget_fraction=require_float("mem_budget_fraction"),
        ya_make_mem_fraction=require_float("ya_make_mem_fraction"),
        mem_min_chunk_gb=require_float("mem_min_chunk_gb"),
        cpu_saturation_pct=require_float("cpu_saturation_pct"),
        near_budget_factor=require_float("near_budget_factor"),
    )


def mem_cpu_tiers_for(max_cores: Optional[int] = None) -> tuple[int, ...]:
    if max_cores is None or max_cores <= 0:
        return _MEM_CPU_TIERS_BASE
    tiers = [t for t in _MEM_CPU_TIERS_BASE if t <= max_cores]
    if not tiers or tiers[-1] != max_cores:
        tiers.append(max_cores)
    return tuple(tiers)


def _first_positive(records: list[dict[str, Any]], key: str) -> Optional[float]:
    for rec in records:
        raw = rec.get(key)
        if raw is None or raw == "":
            continue
        try:
            number = float(raw)
        except (TypeError, ValueError):
            continue
        if number > 0:
            return number
    return None


def measured_host_from_records(records: Optional[list[dict[str, Any]]]) -> Optional[MeasuredHost]:
    """Host CPU/RAM only from monitor records — never from overlay series sentinels."""
    if not records:
        return None
    cpu = _first_positive(records, "cpu_cores")
    ram = _first_positive(records, "ram_total_gb")
    if cpu is None or ram is None:
        return None
    return MeasuredHost(vcpu=max(1, int(round(cpu))), ram_gb=ram)


def enrich_resources_overlay(
    overlay: Optional[dict[str, Any]],
    *,
    records: Optional[list[dict[str, Any]]] = None,
    tuning: Optional[DashboardTuning] = None,
) -> tuple[Optional[dict[str, Any]], Optional[MeasuredHost]]:
    if overlay is None:
        return None, None
    out = dict(overlay)
    host = measured_host_from_records(records)
    if host is None:
        return out, None
    dash = tuning if tuning is not None else load_dashboard_tuning()
    out["host"] = host.to_host_dict(dash)
    return out, host
