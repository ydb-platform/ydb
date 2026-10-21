"""HcSnapshot + per-kind recovery predicates over HealthCheckReporter.last_results.

See health_check.cpp: pdisk/vdisk ids embed node id; GREEN is strict (BLUE=resync);
alive compute = non-empty pools; slots tracked by alive-count, never by runtime id.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import (
    ChaosTarget,
    TargetKind,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import ImpactScope

logger = logging.getLogger(__name__)

# Synthetic ``self_check_result`` values the reporter itself produces when an endpoint fails.
HC_ERROR_RESULTS = frozenset({"HC_REQUEST_ERROR", "HC_RESULT_ERROR"})

GREEN = "GREEN"

# Worst-status merge across hosts' views: an entity counts as GREEN only if every view says GREEN.
_STATUS_RANK = {"GREEN": 0, "BLUE": 1, "YELLOW": 2, "ORANGE": 3, "GREY": 4, "RED": 5}
_UNKNOWN_RANK = 6


def _rank(status) -> int:
    return _STATUS_RANK.get(str(status), _UNKNOWN_RANK)


def _worst(a: str, b: str) -> str:
    return a if _rank(a) >= _rank(b) else b


def _node_id_of(entity_id: str) -> int | None:
    """Node id from a pdisk/vdisk id (``"N-P[-V]"``); None when it does not parse."""
    try:
        return int(str(entity_id).split("-", 1)[0])
    except (TypeError, ValueError):
        return None


@dataclass
class HcSnapshot:
    """One probe-tick view over the reporter's stored healthcheck results."""

    fresh: bool
    data_at: float | None
    answering: frozenset[str]
    # node_id -> entity id -> worst overall across all hosts' views
    storage_by_node: dict[int, dict[str, str]] = field(default_factory=dict)
    alive_compute: int = 0
    clock_skew_green: bool = True
    # host → compute.clock_skew.overall is GREEN (and any node-level skew on that report).
    # Hosts with no clock_skew section are omitted; see host_clock_skew_green.
    clock_skew_green_by_host: dict[str, bool] = field(default_factory=dict)

    def storage_green(self, node_id: int | None) -> bool:
        """True if every observed pdisk/vdisk of ``node_id`` is GREEN (empty → green)."""
        if node_id is None:
            return True
        return all(overall == GREEN for overall in self.storage_by_node.get(node_id, {}).values())

    def host_clock_skew_green(self, host: str) -> bool:
        """Per-host clock skew.

        YDB often omits ``compute.clock_skew`` when there is nothing to report — that must not
        block TimeSkew confirm. Explicit non-GREEN still fails.
        """
        return self.clock_skew_green_by_host.get(host, True)

    def storage_blockers(self, node_id: int | None) -> list[str]:
        """Non-GREEN entities of the node (for stuck reports / UI)."""
        if node_id is None:
            return []
        return [
            f"{entity_id}={overall}"
            for entity_id, overall in sorted(self.storage_by_node.get(node_id, {}).items())
            if overall != GREEN
        ]


def build_snapshot(
    last_results: dict | None,
    *,
    now: float,
    last_update: float | None,
    max_age_sec: float,
) -> HcSnapshot:
    """Merge hosts' HC reports; ``fresh=False`` when data is stale or no endpoint answered."""
    fresh = last_update is not None and (now - last_update) <= max_age_sec
    answering: set[str] = set()
    storage: dict[int, dict[str, str]] = {}
    alive_by_host: list[set[int]] = []
    skew_green = True
    skew_by_host: dict[str, bool] = {}

    for host, entry in (last_results or {}).items():
        if not isinstance(entry, dict):
            continue
        if entry.get("self_check_result") in HC_ERROR_RESULTS:
            continue
        answering.add(host)
        host_alive: set[int] = set()
        seen_skew = False
        host_skew_ok = True
        for db in entry.get("database_status") or []:
            if not isinstance(db, dict):
                continue
            _merge_storage(db.get("storage"), storage)
            compute = db.get("compute")
            if isinstance(compute, dict):
                for node in compute.get("nodes") or []:
                    if not isinstance(node, dict):
                        continue
                    # Alive = non-empty pools (dead dynamic nodes stay GREEN with empty pools).
                    if node.get("pools"):
                        try:
                            host_alive.add(int(node.get("id")))
                        except (TypeError, ValueError):
                            pass
                    node_skew = node.get("clock_skew")
                    if isinstance(node_skew, dict) and node_skew.get("overall") is not None:
                        seen_skew = True
                        if node_skew.get("overall") != GREEN:
                            host_skew_ok = False
                skew = compute.get("clock_skew")
                if isinstance(skew, dict) and skew.get("overall") is not None:
                    seen_skew = True
                    if skew.get("overall") != GREEN:
                        host_skew_ok = False
        if seen_skew:
            skew_by_host[host] = host_skew_ok
            if not host_skew_ok:
                skew_green = False
        alive_by_host.append(host_alive)

    if not answering:
        fresh = False
    # Pessimistic: min across views so one stale "still alive" cannot release early.
    alive_compute = min((len(s) for s in alive_by_host), default=0)
    return HcSnapshot(
        fresh=fresh,
        data_at=last_update,
        answering=frozenset(answering),
        storage_by_node=storage,
        alive_compute=alive_compute,
        clock_skew_green=skew_green,
        clock_skew_green_by_host=skew_by_host,
    )


def _merge_storage(storage_section, out: dict[int, dict[str, str]]) -> None:
    if not isinstance(storage_section, dict):
        return
    for pool in storage_section.get("pools") or []:
        if not isinstance(pool, dict):
            continue
        for group in pool.get("groups") or []:
            if not isinstance(group, dict):
                continue
            for vdisk in group.get("vdisks") or []:
                if not isinstance(vdisk, dict):
                    continue
                _merge_entity(out, vdisk.get("id"), vdisk.get("overall"))
                pdisk = vdisk.get("pdisk")
                if isinstance(pdisk, dict):
                    _merge_entity(out, pdisk.get("id"), pdisk.get("overall"))


def _merge_entity(out: dict[int, dict[str, str]], entity_id, overall) -> None:
    if entity_id is None or overall is None:
        return
    node_id = _node_id_of(entity_id)
    if node_id is None:
        return
    entities = out.setdefault(node_id, {})
    entity_id = str(entity_id)
    overall = str(overall)
    entities[entity_id] = _worst(entities[entity_id], overall) if entity_id in entities else overall


# -- predicates ---------------------------------------------------------------


def node_predicate(target: ChaosTarget) -> Callable[[HcSnapshot], bool]:
    """Endpoint answers and every pdisk/vdisk of the node is GREEN."""

    def recovered(snap: HcSnapshot) -> bool:
        return target.host in snap.answering and snap.storage_green(target.node_id)

    return recovered


def disk_predicate(target: ChaosTarget) -> Callable[[HcSnapshot], bool]:
    """Node-scoped: all of the node's disks are GREEN."""

    def recovered(snap: HcSnapshot) -> bool:
        return target.host in snap.answering and snap.storage_green(target.node_id)

    return recovered


def slot_predicate(baseline: int) -> Callable[[HcSnapshot], bool]:
    """Alive compute count ≥ pre-inject baseline (runtime ids change on restart)."""

    def recovered(snap: HcSnapshot) -> bool:
        return snap.alive_compute >= baseline

    return recovered


def host_predicate(target: ChaosTarget) -> Callable[[HcSnapshot], bool]:
    """Endpoint answers and this host's compute.clock_skew is GREEN."""

    def recovered(snap: HcSnapshot) -> bool:
        return target.host in snap.answering and snap.host_clock_skew_green(target.host)

    return recovered


def datacenter_predicate(
    dc_hosts: list[str], dc_node_ids: list[int]
) -> Callable[[HcSnapshot], bool]:
    """All DC endpoints answer and all node storage is GREEN. Empty lists → never recovered."""

    def recovered(snap: HcSnapshot) -> bool:
        if not dc_hosts or not dc_node_ids:
            return False
        return all(h in snap.answering for h in dc_hosts) and all(
            snap.storage_green(nid) for nid in dc_node_ids
        )

    return recovered


def endpoint_predicate(target: ChaosTarget) -> Callable[[HcSnapshot], bool]:
    """Fallback: host HC endpoint answers."""

    def recovered(snap: HcSnapshot) -> bool:
        return target.host in snap.answering

    return recovered


def needs_baseline(kind: TargetKind, scope: ImpactScope) -> bool:
    """Slot faults need a pre-inject alive-compute baseline on every inject path."""
    return kind is TargetKind.SLOT or scope is ImpactScope.SLOT


def hc_predicate_for(
    target: ChaosTarget,
    *,
    kind: TargetKind,
    scope: ImpactScope,
    inventory=None,
    baseline: int | None = None,
    nemesis_type: str | None = None,
) -> Callable[[HcSnapshot], bool]:
    """Per-kind recovery predicate. Slot without baseline / DC without node ids → never recovers.

    ``TargetKind.HOST`` is shared by TimeSkew / Network / Dns — only TimeSkew waits on clock_skew.
    """
    if needs_baseline(kind, scope):
        if baseline is None:
            logger.error(
                "slot predicate for %s built without a baseline; it will report stuck",
                target.identity_key(),
            )
            return lambda snap: False
        return slot_predicate(baseline)
    if kind is TargetKind.DATACENTER or scope is ImpactScope.DATACENTER:
        dc = target.group_id
        dc_hosts = [target.host]
        dc_node_ids: list[int] = []
        if inventory is not None:
            dc_hosts = sorted(
                {
                    t.host
                    for t in inventory.entities(TargetKind.DATACENTER)
                    if t.group_id == dc
                }
            ) or [target.host]
            nodes = getattr(inventory, "nodes", None) or {}
            dc_node_ids = [n.node_id for n in nodes.values() if n.host in dc_hosts]
            if not dc_node_ids:
                # Test fakes may expose NODE entities without a ``nodes`` map.
                dc_node_ids = [
                    t.node_id
                    for t in inventory.entities(TargetKind.NODE)
                    if t.host in dc_hosts and t.node_id is not None
                ]
        if not dc_node_ids:
            logger.error(
                "datacenter predicate for %s built without node ids; it will report stuck",
                target.identity_key(),
            )
            return lambda snap: False
        return datacenter_predicate(dc_hosts, dc_node_ids)
    if kind is TargetKind.DISK or scope is ImpactScope.DISK:
        return disk_predicate(target)
    if kind is TargetKind.HOST:
        if nemesis_type == "TimeSkewNemesis" or nemesis_type is None:
            return host_predicate(target)
        return endpoint_predicate(target)
    if kind is TargetKind.NODE:
        return node_predicate(target)
    return endpoint_predicate(target)


__all__ = [
    "HC_ERROR_RESULTS",
    "GREEN",
    "HcSnapshot",
    "build_snapshot",
    "hc_predicate_for",
    "needs_baseline",
    "node_predicate",
    "disk_predicate",
    "slot_predicate",
    "host_predicate",
    "datacenter_predicate",
    "endpoint_predicate",
]
