"""Unit tests for ChaosTarget / FailureModelGuard / serial planner core paths."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
import yaml

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    ClusterTopologyModel,
    FailureModelGuard,
    ImpactScope,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.serial_staggered_planner import (
    SerialStaggeredInjectPlanner,
)


def _write_topology_yaml(hosts: list[dict], erasure: str = "block-4-2") -> str:
    doc = {
        "static_erasure": erasure,
        "hosts": hosts,
    }
    _fd, path = tempfile.mkstemp(suffix=".yaml")
    Path(path).write_text(yaml.safe_dump(doc), encoding="utf-8")
    return path


@pytest.fixture
def block42_topology():
    hosts = [
        {"name": "h1", "location": {"rack": "r1", "data_center": "dc1"}},
        {"name": "h2", "location": {"rack": "r2", "data_center": "dc1"}},
        {"name": "h3", "location": {"rack": "r3", "data_center": "dc1"}},
        {"name": "h4", "location": {"rack": "r4", "data_center": "dc1"}},
    ]
    path = _write_topology_yaml(hosts, "block-4-2")
    return ClusterTopologyModel(path)


@pytest.fixture
def mirror3dc_topology():
    # 3 realms (DC), 2 fail domains (racks) each — classic mirror-3-dc layout.
    hosts = [
        {"name": "h1", "location": {"rack": "r1", "data_center": "dc1"}},
        {"name": "h2", "location": {"rack": "r2", "data_center": "dc1"}},
        {"name": "h3", "location": {"rack": "r3", "data_center": "dc2"}},
        {"name": "h4", "location": {"rack": "r4", "data_center": "dc2"}},
        {"name": "h5", "location": {"rack": "r5", "data_center": "dc3"}},
        {"name": "h6", "location": {"rack": "r6", "data_center": "dc3"}},
    ]
    path = _write_topology_yaml(hosts, "mirror-3-dc")
    return ClusterTopologyModel(path)


class TestChaosTarget:
    def test_serde_roundtrip(self):
        t = ChaosTarget.for_node("host-a", node_id=3, ic_port=19001)
        restored = ChaosTarget.from_dict(t.to_dict())
        assert restored == t, (
            f"ChaosTarget serde lost fields: original={t.to_dict()}, restored={restored.to_dict()}"
        )
        assert restored.identity_key() == t.identity_key(), (
            f"identity_key mismatch after serde: {t.identity_key()!r} != {restored.identity_key()!r}"
        )


class TestFailureModelGuard:
    def test_filter_safe_block42_allows_two_domains(self, block42_topology):
        guard = FailureModelGuard(block42_topology)
        assert guard.enabled, (
            f"block-4-2 guard must be enabled; snapshot={guard.snapshot()}"
        )

        c1 = ChaosTarget.for_node("h1", node_id=1)
        c2 = ChaosTarget.for_node("h2", node_id=2)
        c3 = ChaosTarget.for_node("h3", node_id=3)

        safe = guard.filter_safe([c1, c2, c3], ImpactScope.NODE)
        assert {t.host for t in safe} == {"h1", "h2", "h3"}, (
            f"with empty impairments, block-4-2 must allow all candidates; got {[t.host for t in safe]}"
        )

        guard.record_inject("e1", c1, ImpactScope.NODE, recovery_sec=None)
        guard.record_inject("e2", c2, ImpactScope.NODE, recovery_sec=None)

        # Already touched identities are dropped; a third rack exceeds block-4-2 budget (max 2).
        safe_after = guard.filter_safe([c1, c2, c3], ImpactScope.NODE)
        assert safe_after == [], (
            "block-4-2 with 2 impaired racks must reject a third domain and already-touched "
            f"targets; snapshot={guard.snapshot()}, safe_after={[t.host for t in safe_after]}"
        )

    def test_filter_safe_mirror3dc_one_realm_plus_one_domain(self, mirror3dc_topology):
        guard = FailureModelGuard(mirror3dc_topology)
        assert guard.enabled, (
            f"mirror-3-dc guard must be enabled; erasure={mirror3dc_topology.tolerance.erasure!r}, "
            f"snapshot={guard.snapshot()}"
        )
        assert mirror3dc_topology.tolerance.kind == "realm_plus_domain", (
            f"expected realm_plus_domain tolerance for mirror-3-dc, got {mirror3dc_topology.tolerance!r}"
        )

        # Impair whole dc1 (both racks) — one sacrificial realm.
        guard.record_inject(
            "dc1-r1", ChaosTarget.for_node("h1", node_id=1), ImpactScope.NODE, recovery_sec=None
        )
        guard.record_inject(
            "dc1-r2", ChaosTarget.for_node("h2", node_id=2), ImpactScope.NODE, recovery_sec=None
        )

        extra_ok = ChaosTarget.for_node("h3", node_id=3)  # dc2 / r3 — one extra domain
        extra_other = ChaosTarget.for_node("h5", node_id=5)  # dc3 / r5

        # filter_safe checks each candidate independently against current impairments.
        safe = guard.filter_safe([extra_ok, extra_other], ImpactScope.NODE)
        safe_hosts = {t.host for t in safe}
        assert safe_hosts == {"h3", "h5"}, (
            "with only dc1 lost, mirror-3-dc must still allow one extra domain in *either* "
            f"remaining realm (h3 or h5) individually; snapshot={guard.snapshot()}, "
            f"safe={sorted(safe_hosts)}"
        )

        guard.record_inject("e-extra", extra_ok, ImpactScope.NODE, recovery_sec=None)
        safe_after = guard.filter_safe([extra_other], ImpactScope.NODE)
        assert safe_after == [], (
            "after dc1 + one domain in dc2, mirror-3-dc must reject a domain in a third realm "
            f"(h5/dc3); snapshot={guard.snapshot()}, safe_after={[t.host for t in safe_after]}"
        )

    def test_tablet_targets_do_not_consume_budget(self, block42_topology):
        guard = FailureModelGuard(block42_topology)
        tablet = ChaosTarget.for_tablet("h1", tablet_id=42)
        guard.record_inject("t1", tablet, ImpactScope.NODE, recovery_sec=None)
        snap = guard.snapshot()
        assert snap["impaired_racks"] == [], (
            f"tablet inject must not impair racks; snapshot={snap}"
        )
        assert snap["tracked_executions"] == 0, (
            f"tablet inject must not create impairment records; snapshot={snap}"
        )

    def test_extract_releases_budget(self, block42_topology):
        guard = FailureModelGuard(block42_topology)
        t = ChaosTarget.for_host("h1")
        guard.record_inject("e1", t, ImpactScope.NODE, recovery_sec=None)
        assert "r1" in guard.snapshot()["impaired_racks"], (
            f"host inject on h1 must mark rack r1 impaired; snapshot={guard.snapshot()}"
        )
        guard.record_extract("e1", t, ImpactScope.NODE)
        assert guard.snapshot()["impaired_racks"] == [], (
            f"extract must release impaired racks; snapshot={guard.snapshot()}"
        )


class TestSerialStaggeredPlanner:
    def test_dispatches_only_to_owner_host(self):
        planner = SerialStaggeredInjectPlanner("SerialKillNodeNemesis", target_kind="node")
        candidates = [
            ChaosTarget.for_node("h1", node_id=1, ic_port=19001),
            ChaosTarget.for_node("h2", node_id=2, ic_port=19001),
        ]
        cmds = planner.scheduled_tick(candidates)
        assert len(cmds) == 1, (
            f"serial planner must emit exactly one command for one chosen node; got {len(cmds)}"
        )
        cmd = cmds[0]
        assert cmd.target.host in {"h1", "h2"}, (
            f"chosen host must be from candidates; got {cmd.target.host!r}"
        )
        assert cmd.target.node_id in {1, 2}, (
            f"chosen node_id must be from candidates; got {cmd.target.node_id!r}"
        )
        assert cmd.payload.get("node_id") == cmd.target.node_id, (
            f"payload.node_id must match ChaosTarget.node_id; "
            f"payload={cmd.payload}, target={cmd.target.to_dict()}"
        )
        assert cmd.host == cmd.target.host, (
            f"dispatch host must be the owner of the chosen node, not a random host; "
            f"host={cmd.host!r}, target.host={cmd.target.host!r}"
        )
