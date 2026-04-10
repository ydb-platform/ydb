"""Lazy ``ExternalKiKiMRCluster`` for YAML-backed chaos on agents."""

from __future__ import annotations

import os
from functools import lru_cache

from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster
from ydb.tests.stability.nemesis.internal.config import Settings


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
