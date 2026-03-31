"""Lazy ``ExternalKiKiMRCluster`` for YAML-backed chaos on agents."""

from __future__ import annotations

import os
from functools import lru_cache

import yaml

from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster
from ydb.tests.stability.nemesis.internal.config import Settings


def cluster_yaml_path() -> str:
    return (os.environ.get("YAML_CONFIG_LOCATION") or Settings().yaml_config_location or "").strip()


@lru_cache(maxsize=1)
def _cluster_for_path(path: str) -> ExternalKiKiMRCluster:
    """
    If the file is a unified template + deploy doc (``domains`` + ``config.hosts``),
    pass it as ``yaml_config`` too so ``bridge_pile_id`` / host locations resolve.
    """
    with open(path, "r", encoding="utf-8") as r:
        doc = yaml.safe_load(r.read())
    if (
        isinstance(doc, dict)
        and doc.get("domains")
        and isinstance(doc.get("config"), dict)
        and isinstance(doc["config"].get("hosts"), list)
    ):
        return ExternalKiKiMRCluster(path, None, None, yaml_config=path)
    return ExternalKiKiMRCluster(path, None, None)


def require_external_cluster() -> ExternalKiKiMRCluster:
    path = cluster_yaml_path()
    if not path:
        raise RuntimeError(
            "Cluster chaos requires cluster.yaml: set YAML_CONFIG_LOCATION to its absolute path on this host."
        )
    return _cluster_for_path(path)


def load_external_cluster_optional() -> ExternalKiKiMRCluster | None:
    """Orchestrator-side: load cluster when YAML path is set; otherwise ``None``."""
    path = cluster_yaml_path()
    if not path:
        return None
    try:
        return _cluster_for_path(path)
    except Exception:
        return None
