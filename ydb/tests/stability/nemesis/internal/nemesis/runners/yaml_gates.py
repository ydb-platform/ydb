"""Enable optional cluster nemeses when cluster.yaml contains the required layout."""

from __future__ import annotations

from pathlib import Path

import yaml


def _load_cluster_doc(path: str) -> dict | None:
    if not path or not Path(path).is_file():
        return None
    with open(path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f.read())
    return doc if isinstance(doc, dict) else None


def yaml_has_multi_datacenter(path: str) -> bool:
    """At least two distinct ``location.data_center`` values among hosts."""
    doc = _load_cluster_doc(path)
    if not doc:
        return False
    hosts = doc.get("hosts")
    if not isinstance(hosts, list) or not hosts:
        hosts = doc.get("config", {}).get("hosts", [])
    if not isinstance(hosts, list):
        return False
    dcs: set[object] = set()
    for h in hosts:
        if not isinstance(h, dict):
            continue
        loc = h.get("location") or {}
        if not isinstance(loc, dict):
            continue
        dc = loc.get("data_center")
        if dc is not None:
            dcs.add(dc)
    return len(dcs) >= 2


def yaml_has_bridge_piles_section(path: str) -> bool:
    """``config.bridge_config`` with at least two piles (same shape as harness)."""
    doc = _load_cluster_doc(path)
    if not doc:
        return False
    piles = doc.get("config", {}).get("bridge_config", {}).get("piles", [])
    return isinstance(piles, list) and len(piles) >= 2
