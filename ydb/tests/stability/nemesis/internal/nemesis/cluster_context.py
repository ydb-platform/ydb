"""Cluster access for production agents and embedded harness execution."""

from __future__ import annotations

import os
from contextlib import contextmanager
from functools import lru_cache
from typing import Any, Iterator

from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster
from ydb.tests.stability.nemesis.internal.config import Settings


_cluster_override: Any | None = None

# Start deliberately narrow. Each additional type needs its own local scenario
# test before it is admitted here.
LOCAL_HARNESS_NEMESIS_TYPES = frozenset({
    "KillDataShardNemesis",
    "KillHiveNemesis",
    "KillNodeNemesis",
    "KillSchemeShardNemesis",
    "KillSlotDaemonNemesis",
    "SafelyBreakDiskNemesis",
    "StopStartNodeNemesis",
    "SuspendNodeNemesis",
})

LOCAL_HARNESS_SCHEDULED_NEMESIS_TYPES = frozenset({
    "KillDataShardNemesis",
    "KillNodeNemesis",
    "KillSlotDaemonNemesis",
    "SafelyBreakDiskNemesis",
    "StopStartNodeNemesis",
    "SuspendNodeNemesis",
})

LOCAL_HARNESS_BLOCK_4_2_PROCESS_FAULT_TYPES = frozenset({
    "KillNodeNemesis",
    "KillSlotDaemonNemesis",
    "StopStartNodeNemesis",
    "SuspendNodeNemesis",
})


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


def local_harness_mode() -> bool:
    return _cluster_override is not None


def nemesis_rejection_reason(nemesis_type: str) -> str | None:
    """Why a type cannot execute in the current runtime, if applicable."""
    if local_harness_mode() and nemesis_type not in LOCAL_HARNESS_NEMESIS_TYPES:
        return (
            f"{nemesis_type} is not enabled for local harness execution; "
            f"allowed types: {', '.join(sorted(LOCAL_HARNESS_NEMESIS_TYPES))}"
        )
    return None


def scheduled_nemesis_rejection_reason(nemesis_type: str) -> str | None:
    rejection = nemesis_rejection_reason(nemesis_type)
    if rejection is not None:
        return rejection
    if (
        local_harness_mode()
        and nemesis_type in LOCAL_HARNESS_BLOCK_4_2_PROCESS_FAULT_TYPES
    ):
        nodes = getattr(_cluster_override, "nodes", {}) or {}
        erasure = str(
            getattr(getattr(_cluster_override, "config", None), "static_erasure", "")
        ).lower()
        if erasure != "block-4-2" or len(nodes) < 8:
            return (
                f"{nemesis_type} requires at least 8 block-4-2 nodes in local "
                f"harness mode; got erasure={erasure or 'unknown'}, nodes={len(nodes)}"
            )
    if local_harness_mode() and nemesis_type == "SafelyBreakDiskNemesis":
        nodes = getattr(_cluster_override, "nodes", {}) or {}
        config = getattr(_cluster_override, "config", None)
        erasure = str(getattr(config, "static_erasure", "")).lower()
        pdisks_per_node = {}
        dedicated_spares_per_node = {}
        for pdisk in getattr(config, "pdisks_info", ()) or ():
            node_id = pdisk.get("node_id")
            pdisks_per_node[node_id] = pdisks_per_node.get(node_id, 0) + 1
            if pdisk.get("pdisk_user_kind", 0) != 0:
                dedicated_spares_per_node[node_id] = (
                    dedicated_spares_per_node.get(node_id, 0) + 1
                )
        has_spare_in_each_domain = bool(nodes) and all(
            pdisks_per_node.get(node_id, 0) >= 2
            and dedicated_spares_per_node.get(node_id, 0) >= 1
            for node_id in nodes
        )
        if (
            erasure != "block-4-2"
            or len(nodes) < 8
            or not has_spare_in_each_domain
        ):
            return (
                "SafelyBreakDiskNemesis requires at least 8 block-4-2 nodes "
                "and a dedicated non-default-kind spare PDisk per node in "
                "local harness mode; "
                f"got erasure={erasure or 'unknown'}, nodes={len(nodes)}, "
                f"pdisks_per_node={pdisks_per_node}, "
                f"dedicated_spares_per_node={dedicated_spares_per_node}"
            )
    if local_harness_mode() and nemesis_type == "KillSlotDaemonNemesis":
        slots = getattr(_cluster_override, "slots", {}) or {}
        if not slots:
            return "KillSlotDaemonNemesis requires live local harness slots"
    if (
        local_harness_mode()
        and nemesis_type not in LOCAL_HARNESS_SCHEDULED_NEMESIS_TYPES
    ):
        return f"{nemesis_type} is not enabled in the local harness scheduled profile"
    return None


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
