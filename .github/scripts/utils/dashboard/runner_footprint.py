"""Runner capacity for the tests dashboard: measured host specs only.

Chart limit lines and CPU/memory recommendations use /proc data from
``resources_monitor.jsonl``. Provisioned sizes in ``runners_footprints.yml``
are not used here (that file remains for fleet capacity export).

Dashboard tuning knobs (budget fractions, saturation) may still be read from
the ``dashboard:`` section of that YAML.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

CONFIG_REL_PATH = Path(".github/config/runners_footprints.yml")


@dataclass(frozen=True)
class RunnerCapacity:
    """Actual runner capacity from the host monitor, plus dashboard tuning."""

    vcpu: int
    ram_gb: float
    mem_budget_gb: float
    mem_min_chunk_gb: float
    cpu_saturation_pct: float
    near_budget_factor: float
    mem_cpu_tiers: tuple[int, ...]
    ya_make_mem_limit_gb: float
    source: str = "measured"

    def to_dict(self) -> dict[str, Any]:
        return {
            "vcpu": self.vcpu,
            "ram_gb": round(self.ram_gb, 1),
            "mem_budget_gb": round(self.mem_budget_gb, 1),
            "mem_min_chunk_gb": self.mem_min_chunk_gb,
            "cpu_saturation_pct": self.cpu_saturation_pct,
            "near_budget_factor": self.near_budget_factor,
            "ya_make_mem_limit_gb": round(self.ya_make_mem_limit_gb, 1),
            "source": self.source,
        }


# Backward-compatible alias for older imports/tests.
RunnerFootprint = RunnerCapacity


def _repo_root_from_here() -> Path:
    p = Path(__file__).resolve().parent
    for _ in range(12):
        if (p / "ydb").is_dir() and (p / ".github").is_dir():
            return p
        if p.parent == p:
            break
        p = p.parent
    return Path.cwd()


def default_config_path() -> Path:
    root = _repo_root_from_here()
    candidate = root / CONFIG_REL_PATH
    if candidate.is_file():
        return candidate
    return CONFIG_REL_PATH


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError("PyYAML is required to load runners_footprints.yml") from exc
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, dict) else {}


def mem_cpu_tiers_for(max_cores: int) -> tuple[int, ...]:
    base = (1, 2, 4, 8, 16, 32, 48, 64, 96, 128)
    tiers = [t for t in base if t <= max_cores]
    if max_cores > 0 and (not tiers or tiers[-1] != max_cores):
        tiers.append(max_cores)
    return tuple(tiers)


def _dashboard_defaults() -> dict[str, float]:
    return {
        "mem_budget_fraction": 0.70,
        "mem_min_chunk_gb": 24.0,
        "cpu_saturation_pct": 90.0,
        "near_budget_factor": 1.10,
        "ya_make_mem_fraction": 0.95,
    }


def load_dashboard_tuning(config_path: Optional[Path] = None) -> dict[str, float]:
    """Load dashboard tuning knobs (fractions/thresholds), not machine sizes."""
    dash = dict(_dashboard_defaults())
    path = config_path or default_config_path()
    try:
        config = _load_yaml(path) if path.is_file() else {}
    except (OSError, RuntimeError):
        return dash
    raw = config.get("dashboard")
    if isinstance(raw, dict):
        for k in dash:
            if k in raw:
                try:
                    dash[k] = float(raw[k])
                except (TypeError, ValueError):
                    pass
    return dash


def capacity_from_measured(
    *,
    cpu_cores: Optional[float],
    ram_gb: Optional[float],
    config_path: Optional[Path] = None,
) -> Optional[RunnerCapacity]:
    """Build capacity from measured host specs. Returns None if either is missing."""
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

    dash = load_dashboard_tuning(config_path)
    vcpu = max(1, int(round(cpu)))
    return RunnerCapacity(
        vcpu=vcpu,
        ram_gb=ram,
        mem_budget_gb=ram * float(dash["mem_budget_fraction"]),
        mem_min_chunk_gb=float(dash["mem_min_chunk_gb"]),
        cpu_saturation_pct=float(dash["cpu_saturation_pct"]),
        near_budget_factor=float(dash["near_budget_factor"]),
        mem_cpu_tiers=mem_cpu_tiers_for(vcpu),
        ya_make_mem_limit_gb=ram * float(dash["ya_make_mem_fraction"]),
        source="measured",
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
    """Read cpu_cores / ram_total_gb from monitor records or overlay fields."""
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
    config_path: Optional[Path] = None,
) -> tuple[Optional[dict[str, Any]], Optional[RunnerCapacity]]:
    """Attach measured host limits. No fake provisioned ceilings when monitor is absent."""
    if overlay is None:
        return None, None

    out = dict(overlay)
    measured = extract_measured_host_specs(out, records=records)
    out["measured"] = measured

    capacity = capacity_from_measured(
        cpu_cores=measured.get("cpu_cores"),
        ram_gb=measured.get("ram_gb"),
        config_path=config_path,
    )
    if capacity is None:
        # Do not invent limits — charts/recommendations skip them.
        out.pop("runner_limits", None)
        out.pop("runner_capacity", None)
        out.pop("runner_footprint", None)
        return out, None

    out["runner_limits"] = {
        "cpu_cores_max": capacity.vcpu,
        "ram_gb_max": capacity.ram_gb,
        "mem_budget_gb": round(capacity.mem_budget_gb, 1),
        "ya_make_mem_limit_gb": round(capacity.ya_make_mem_limit_gb, 1),
    }
    capacity_dict = capacity.to_dict()
    out["runner_capacity"] = capacity_dict
    # Keep legacy key so older HTML/JSON consumers still see the same fields.
    out["runner_footprint"] = capacity_dict
    return out, capacity
