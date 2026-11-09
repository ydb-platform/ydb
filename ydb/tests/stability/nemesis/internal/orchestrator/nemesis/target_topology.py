"""Resolve logical resource placement separately from the agent's network address."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget, TargetKind


def fail_domain_key(datacenter: str | None, rack: str, pile_id: int | None = None) -> str:
    domain = f"{datacenter or '?'}/{rack}"
    return domain if pile_id is None else f"pile:{pile_id}/{domain}"


@dataclass(frozen=True)
class Placement:
    host: str
    rack: str
    datacenter: str | None
    pile_id: int | None

    @property
    def domain(self) -> str:
        return fail_domain_key(self.datacenter, self.rack, self.pile_id)


class TargetTopology:
    """A snapshot of authoritative placements; request payloads do not supply topology.

    A node/slot on localhost can belong to a different pile from another process
    at the same address. Whole-host actions resolve to all resources at that address.
    """

    def __init__(self, hosts, *, nodes=(), slots=(), pile_names=None):
        self.pile_names = dict(pile_names or {})
        self.hosts = dict(hosts)
        self.nodes = list(nodes)
        self.slots = list(slots)
        self.domain_to_pile = {}
        self.domain_to_dc = {}
        for resource in [*self.hosts.values(), *self.nodes, *self.slots]:
            placement = self.placement(resource)
            self.domain_to_pile[placement.domain] = placement.pile_id
            self.domain_to_dc[placement.domain] = placement.datacenter
        self.slots_per_pile = dict(Counter(self.placement(s).pile_id for s in self.slots))

    def placement(self, resource) -> Placement:
        fallback = self.hosts.get(resource.host)
        rack = resource.rack if resource.rack is not None else getattr(fallback, "rack", None)
        dc = resource.datacenter if resource.datacenter is not None else getattr(fallback, "datacenter", None)
        pile = resource.bridge_pile_id
        if pile is None:
            pile = getattr(fallback, "bridge_pile_id", None)
        if self.pile_names and pile not in self.pile_names:
            raise ValueError(f"Unknown bridge pile for resource on {resource.host}: {pile}")
        if rack is None:
            raise ValueError(f"Unknown rack for resource on {resource.host}")
        return Placement(resource.host, str(rack), dc, pile)

    def _single(self, target, records, *, slot=False):
        def matches(record):
            if record.host != target.host:
                return False
            if slot and target.slot_idx is not None:
                return record.slot_idx == target.slot_idx
            if target.node_id is not None:
                return record.node_id == target.node_id
            return target.ic_port is not None and record.ic_port == target.ic_port

        found = [r for r in records if matches(r)]
        if len(found) == 1:
            return found
        if records or self.pile_names:
            raise ValueError(f"Cannot resolve unique resource placement: {target.identity_key()}")
        # Legacy host-only inventory for a non-bridge cluster.
        return [self.hosts[target.host]] if target.host in self.hosts else []

    def resolve(self, target: ChaosTarget, scope: str):
        """Return storage domains and slot counts by pile for the actual impact."""
        if target.kind is TargetKind.TABLET:
            return frozenset(), ()
        if target.kind is TargetKind.SLOT or scope == "slot":
            resources = self._single(target, self.slots, slot=True)
            if not resources:  # legacy non-bridge unknown inventory
                return frozenset(), ((None, 1),)
            return frozenset(), ((self.placement(resources[0]).pile_id, 1),)

        storage = self.nodes or list(self.hosts.values())
        affected_slots = []
        if target.kind is TargetKind.PILE or scope == "pile":
            def belongs(r):
                return str(self.placement(r).pile_id) == target.group_id
            resources = [r for r in storage if belongs(r)]
            affected_slots = [r for r in self.slots if belongs(r)]
        elif target.kind is TargetKind.DATACENTER or scope == "datacenter":
            dc = target.group_id or getattr(self.hosts.get(target.host), "datacenter", None)
            resources = [r for r in storage if self.placement(r).datacenter == dc]
            affected_slots = [r for r in self.slots if self.placement(r).datacenter == dc]
        elif target.kind is TargetKind.HOST:
            resources = [r for r in storage if r.host == target.host]
            affected_slots = [r for r in self.slots if r.host == target.host]
        else:
            resources = self._single(target, self.nodes)
        if not resources:
            if self.pile_names:
                raise ValueError(f"No resources for {target.identity_key()}")
            fallback = self.hosts.get(target.host)
            dc = getattr(fallback, "datacenter", None)
            domains = frozenset({f"__host__:{fail_domain_key(dc, target.host)}"})
        else:
            domains = frozenset(self.placement(r).domain for r in resources)
        counts = Counter(self.placement(r).pile_id for r in affected_slots)
        return domains, tuple(counts.items())
