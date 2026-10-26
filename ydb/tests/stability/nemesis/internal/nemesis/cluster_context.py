"""Cluster access for production agents and embedded harness execution."""

from __future__ import annotations

import os
from contextlib import contextmanager
from functools import lru_cache
from typing import Any, Iterator

from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster
from ydb.tests.stability.nemesis.internal.config import Settings


_cluster_override: Any | None = None


def cluster_yaml_path() -> str:
    return (os.environ.get("YAML_CONFIG_LOCATION") or Settings().yaml_config_location or "").strip()


def database_yaml_path() -> str:
    return (os.environ.get("DATABASE_CONFIG_LOCATION") or Settings().database_config_location or "").strip()


@lru_cache(maxsize=1)
def _cluster_for_path(path: str, database_path: str) -> ExternalKiKiMRCluster:
    cluster_yaml = None
    template_yaml = path
    if database_path:
        template_yaml = database_path
        cluster_yaml = path

    return ExternalKiKiMRCluster(template_yaml, None, None, yaml_config=cluster_yaml)


def require_external_cluster() -> ExternalKiKiMRCluster:
    path = cluster_yaml_path()
    database_path = database_yaml_path()
    if not path:
        raise RuntimeError(
            "Cluster chaos requires cluster.yaml: set YAML_CONFIG_LOCATION to its absolute path on this host."
        )
    return _cluster_for_path(path, database_path)


def require_cluster() -> Any:
    """Return the embedded harness cluster when set, otherwise the production cluster."""
    if _cluster_override is not None:
        return _cluster_override
    return require_external_cluster()


@contextmanager
def use_cluster(cluster: Any) -> Iterator[Any]:
    """Temporarily expose a live harness cluster to local nemesis runners.

    The override is process-wide so runner threads spawned by ``NemesisManager``
    see the same cluster. Production agents never install an override.
    """
    global _cluster_override
    previous = _cluster_override
    _cluster_override = cluster
    try:
        yield cluster
    finally:
        _cluster_override = previous
