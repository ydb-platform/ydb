"""Load runner provisioned footprints and dashboard tuning from .github/config."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

CONFIG_REL_PATH = Path(".github/config/runners_footprints.yml")


@dataclass(frozen=True)
class RunnerFootprint:
    """Provisioned runner capacity (max) plus derived dashboard budgets."""

    vcpu: int
    ram_gb: float
    mem_budget_gb: float
    mem_min_chunk_gb: float
    cpu_saturation_pct: float
    near_budget_factor: float
    mem_cpu_tiers: tuple[int, ...]
    ya_make_mem_limit_gb: float
    build_preset: Optional[str] = None
    footprint_key: Optional[str] = None
    source: str = "config"

    def to_dict(self) -> dict[str, Any]:
        return {
            "vcpu": self.vcpu,
            "ram_gb": round(self.ram_gb, 1),
            "mem_budget_gb": round(self.mem_budget_gb, 1),
            "mem_min_chunk_gb": self.mem_min_chunk_gb,
            "cpu_saturation_pct": self.cpu_saturation_pct,
            "near_budget_factor": self.near_budget_factor,
            "ya_make_mem_limit_gb": round(self.ya_make_mem_limit_gb, 1),
            "build_preset": self.build_preset,
            "footprint_key": self.footprint_key,
            "source": self.source,
        }


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


def footprint_key_for_preset(build_preset: str) -> str:
    preset = (build_preset or "").strip()
    if not preset:
        return ""
    if preset.startswith("build-preset-"):
        return preset
    return f"build-preset-{preset}"


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


def _pick_footprint_entry(
    config: dict[str, Any],
    build_preset: Optional[str],
) -> tuple[dict[str, Any], str, str]:
    footprints = config.get("footprints") or {}
    if not isinstance(footprints, dict):
        footprints = {}

    key = footprint_key_for_preset(build_preset or "")
    if key and key in footprints and isinstance(footprints[key], dict):
        return footprints[key], key, "config"

    default_fp = config.get("default_footprint") or {}
    if isinstance(default_fp, dict) and default_fp:
        return default_fp, "default_footprint", "default"

    return {"vcpu": 96, "ram_gb": 320.0}, "fallback", "fallback"


def resolve_runner_footprint(
    *,
    build_preset: Optional[str] = None,
    config_path: Optional[Path] = None,
) -> RunnerFootprint:
    """Resolve provisioned runner maximums for recommendations and chart limit lines."""
    path = config_path or default_config_path()
    try:
        config = _load_yaml(path) if path.is_file() else {}
    except OSError:
        config = {}

    fp_entry, fp_key, source = _pick_footprint_entry(config, build_preset)
    dash = dict(_dashboard_defaults())
    raw_dash = config.get("dashboard")
    if isinstance(raw_dash, dict):
        for k in dash:
            if k in raw_dash:
                try:
                    dash[k] = float(raw_dash[k])
                except (TypeError, ValueError):
                    pass

    vcpu = int(fp_entry.get("vcpu") or 1)
    ram_gb = float(fp_entry.get("ram_gb") or 1.0)
    mem_budget_gb = ram_gb * float(dash["mem_budget_fraction"])
    ya_make_mem_limit_gb = ram_gb * float(dash["ya_make_mem_fraction"])

    return RunnerFootprint(
        vcpu=max(1, vcpu),
        ram_gb=ram_gb,
        mem_budget_gb=mem_budget_gb,
        mem_min_chunk_gb=float(dash["mem_min_chunk_gb"]),
        cpu_saturation_pct=float(dash["cpu_saturation_pct"]),
        near_budget_factor=float(dash["near_budget_factor"]),
        mem_cpu_tiers=mem_cpu_tiers_for(max(1, vcpu)),
        ya_make_mem_limit_gb=ya_make_mem_limit_gb,
        build_preset=(build_preset or None),
        footprint_key=fp_key,
        source=source,
    )


def enrich_resources_overlay(
    overlay: Optional[dict[str, Any]],
    footprint: RunnerFootprint,
    *,
    records: Optional[list[dict[str, Any]]] = None,
) -> Optional[dict[str, Any]]:
    """Attach provisioned limits and measured machine specs to the monitor overlay."""
    if overlay is None:
        return None
    out = dict(overlay)
    out["runner_limits"] = {
        "cpu_cores_max": footprint.vcpu,
        "ram_gb_max": footprint.ram_gb,
        "mem_budget_gb": round(footprint.mem_budget_gb, 1),
        "ya_make_mem_limit_gb": round(footprint.ya_make_mem_limit_gb, 1),
    }
    measured_cpu = out.get("cpu_cores")
    measured_ram_gb: Optional[float] = None
    recs = records or []
    for rec in recs:
        v = rec.get("ram_total_gb")
        if v is not None:
            try:
                measured_ram_gb = float(v)
                break
            except (TypeError, ValueError):
                continue
    out["measured"] = {
        "cpu_cores": measured_cpu,
        "ram_gb": measured_ram_gb,
    }
    out["runner_footprint"] = footprint.to_dict()
    return out
