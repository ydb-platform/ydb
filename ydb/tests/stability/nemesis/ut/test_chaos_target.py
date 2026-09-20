"""ChaosTarget, the failure-model guard and the serial planner."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    ClusterTopologyModel,
    FailureModelConfigError,
    FailureModelGuard,
    ImpactScope,
    fail_domain_key,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.serial_staggered_planner import (
    DEFAULT_SERIAL_STAGGER_SEC,
    MAX_ENTITIES_PER_TICK,
    SerialStaggeredInjectPlanner,
)


def _topology_yaml(hosts: list[dict], erasure: str = "block-4-2") -> str:
    fd, path = tempfile.mkstemp(suffix=".yaml")
    os.close(fd)
    Path(path).write_text(
        yaml.safe_dump({"static_erasure": erasure, "hosts": hosts}), encoding="utf-8"
    )
    return path


def _hosts(spec: list[tuple[str, str, str | None]]) -> list[dict]:
    out = []
    for name, rack, dc in spec:
        location = {"rack": rack}
        if dc is not None:
            location["data_center"] = dc
        out.append({"name": name, "location": location})
    return out


@pytest.fixture
def block42_topology():
    hosts = _hosts([(f"h{i}", f"r{i}", "dc1") for i in (1, 2, 3, 4)])
    return ClusterTopologyModel(_topology_yaml(hosts, "block-4-2"))


@pytest.fixture
def mirror3dc_topology():
    """3 realms × 2 domains, with rack labels repeating per DC — as real configs do."""
    hosts = _hosts(
        [(f"{dc}-{name}", rack, dc) for dc in ("dc1", "dc2", "dc3") for name, rack in (("a", "1"), ("b", "2"))]
    )
    return ClusterTopologyModel(_topology_yaml(hosts, "mirror-3-dc"))


def test_chaos_target_serde_roundtrip():
    t = ChaosTarget.for_node("host-a", node_id=3, ic_port=19001)
    restored = ChaosTarget.from_dict(t.to_dict())
    assert restored == t and restored.identity_key() == t.identity_key()


class TestFailureModelIsMandatory:
    """An unusable config raises instead of degrading into a guard that permits everything."""

    def test_missing_or_unusable_config(self, tmp_path):
        bad_yaml = tmp_path / "bad.yaml"
        bad_yaml.write_text("static_erasure: [unclosed\n", encoding="utf-8")
        not_a_mapping = tmp_path / "list.yaml"
        not_a_mapping.write_text("- a\n- b\n", encoding="utf-8")
        cases = [
            "",
            "/nonexistent/cluster.yaml",
            str(bad_yaml),
            str(not_a_mapping),
            _topology_yaml(_hosts([("h1", "r1", "dc1")]), "no-such-erasure"),
            _topology_yaml([], "block-4-2"),
            _topology_yaml([{"name": "h1"}], "block-4-2"),  # no location
            _topology_yaml([{"name": "h1", "location": {"data_center": "dc1"}}], "block-4-2"),  # no rack
        ]
        for path in cases:
            with pytest.raises(FailureModelConfigError):
                ClusterTopologyModel(path)

    @pytest.mark.parametrize(
        "doc",
        [
            {"static_erasure": "block-4-2", "hosts": _hosts([("h1", "r1", "dc1")])},
            {"erasure": "block-4-2", "hosts": _hosts([("h1", "r1", "dc1")])},
            {"config": {"static_erasure": "block-4-2", "hosts": _hosts([("h1", "r1", "dc1")])}},
            {"config": {"erasure": "block-4-2", "hosts": _hosts([("h1", "r1", "dc1")])}},
        ],
        ids=["top_static_erasure", "top_erasure", "nested_static_erasure", "nested_erasure"],
    )
    def test_accepts_every_erasure_spelling_and_nesting(self, tmp_path, doc):
        """``ydb/tools/cfg`` takes ``static_erasure`` or ``erasure``, and a V2 config nests both the
        erasure mode and ``hosts`` under ``config:`` — all four must build the same model."""
        path = tmp_path / "cluster.yaml"
        path.write_text(yaml.safe_dump(doc), encoding="utf-8")
        topology = ClusterTopologyModel(str(path))
        assert topology.tolerance.erasure == "block-4-2"
        assert topology.domain_of("h1") == fail_domain_key("dc1", "r1")

    def test_missing_erasure_names_where_it_looked(self, tmp_path):
        path = tmp_path / "cluster.yaml"
        path.write_text(
            yaml.safe_dump({"config": {"hosts": _hosts([("h1", "r1", "dc1")])}}), encoding="utf-8"
        )
        with pytest.raises(FailureModelConfigError, match="no erasure mode found"):
            ClusterTopologyModel(str(path))

    def test_mirror3dc_requires_datacenter(self):
        # Realms may be sacrificed whole, so a host with an unknown realm is not decidable.
        with pytest.raises(FailureModelConfigError, match="data_center"):
            ClusterTopologyModel(_topology_yaml(_hosts([("h1", "r1", None)]), "mirror-3-dc"))

    def test_erasure_none_forbids_everything(self):
        guard = FailureModelGuard(
            ClusterTopologyModel(_topology_yaml(_hosts([("h1", "r1", "dc1")]), "none"))
        )
        fp = guard.footprint_for(ChaosTarget.for_node("h1", node_id=1), ImpactScope.NODE)
        assert guard.reserve(fp) is None, "no redundancy means no fault is tolerable"


class TestFailDomainKeying:
    """Rack labels repeat per datacenter, so a domain must be keyed by (dc, rack)."""

    def test_same_rack_label_in_different_dcs_are_distinct(self, mirror3dc_topology):
        assert mirror3dc_topology.domain_of("dc1-a") != mirror3dc_topology.domain_of("dc2-a")
        assert mirror3dc_topology.domains_in_dc("dc1") == {
            fail_domain_key("dc1", "1"),
            fail_domain_key("dc1", "2"),
        }

    def test_mirror3dc_rejects_a_fault_in_a_third_realm(self, mirror3dc_topology):
        guard = FailureModelGuard(mirror3dc_topology)
        for host in ("dc1-a", "dc2-a"):  # rack '1' in two different DCs
            target = ChaosTarget.for_node(host, node_id=1)
            assert guard.reserve(
                guard.footprint_for(target, ImpactScope.NODE), identity_key=target.identity_key()
            ), "one domain per realm in two realms must fit"
        assert len(guard.snapshot()["impaired_racks"]) == 2, "same-labelled racks are two domains"

        third = ChaosTarget.for_node("dc3-a", node_id=1)
        third_fp = guard.footprint_for(third, ImpactScope.NODE)
        assert guard.reserve(third_fp) is None, "1 realm + 1 domain leaves no room for a third realm"
        assert guard.filter_safe([third], ImpactScope.NODE) == [], "filter_safe must agree"

    def test_datacenter_footprint_covers_only_its_own_realm(self, mirror3dc_topology):
        guard = FailureModelGuard(mirror3dc_topology)
        fp = guard.footprint_for(ChaosTarget.for_datacenter("dc1-a", "dc1"), ImpactScope.DATACENTER)
        assert set(fp.racks) == {fail_domain_key("dc1", "1"), fail_domain_key("dc1", "2")}

    def test_unknown_hosts_get_realm_aware_synthetic_domains(self, mirror3dc_topology):
        # Hosts absent from cluster.yaml must not collapse into one sacrificial realm.
        guard = FailureModelGuard(mirror3dc_topology)
        keys = {
            host: set(guard.footprint_for(ChaosTarget.for_host(host), ImpactScope.NODE).racks).pop()
            for host in ("ghost-a", "ghost-b")
        }
        assert keys["ghost-a"] != keys["ghost-b"], keys
        assert all(k.startswith("__host__:") for k in keys.values()), keys


class TestFilterSafeAndRecording:
    def test_filter_safe_block42_admits_two_domains(self, block42_topology):
        guard = FailureModelGuard(block42_topology)
        candidates = [ChaosTarget.for_node(f"h{i}", node_id=i) for i in (1, 2, 3)]
        safe = guard.filter_safe(candidates, ImpactScope.NODE)
        assert {t.host for t in safe} == {"h1", "h2"}, "the budget is 2 domains"

    def test_filter_safe_independent_keeps_all_individually_ok(self, block42_topology):
        guard = FailureModelGuard(block42_topology)
        candidates = [ChaosTarget.for_node(f"h{i}", node_id=i) for i in (1, 2, 3)]
        safe = guard.filter_safe(candidates, ImpactScope.NODE, jointly=False)
        assert {t.host for t in safe} == {"h1", "h2", "h3"}

    def test_filter_safe_mirror3dc_one_realm_plus_one_domain(self, mirror3dc_topology):
        guard = FailureModelGuard(mirror3dc_topology)
        for i, host in enumerate(("dc1-a", "dc1-b"), start=1):  # sacrifice dc1 entirely
            guard.record_inject(
                f"dc1-{i}", ChaosTarget.for_node(host, node_id=i), ImpactScope.NODE
            )
        extra = [ChaosTarget.for_node("dc2-a", node_id=3), ChaosTarget.for_node("dc3-a", node_id=5)]
        safe = guard.filter_safe(extra, ImpactScope.NODE)
        assert [t.host for t in safe] == ["dc2-a"], "only one extra domain fits after a lost realm"

    def test_untracked_extract_drops_by_identity_not_by_domain(self, block42_topology):
        guard = FailureModelGuard(block42_topology)
        n1 = ChaosTarget.for_node("h1", node_id=1)
        n2 = ChaosTarget.for_node("h1", node_id=2)  # same host, same domain
        guard.record_inject("tracked", n1, ImpactScope.NODE)
        guard.record_extract("untracked-other", n2, ImpactScope.NODE)
        assert "dc1/r1" in guard.snapshot()["impaired_racks"], "n1 must survive n2's extract"
        guard.record_extract("tracked", n1, ImpactScope.NODE)
        assert guard.snapshot()["impaired_racks"] == []

    def test_tablet_faults_consume_nothing(self, block42_topology):
        guard = FailureModelGuard(block42_topology)
        tablet = ChaosTarget.for_tablet("h1", tablet_id=42)
        assert not guard.footprint_for(tablet, ImpactScope.NODE)
        guard.record_inject("t1", tablet, ImpactScope.NODE)
        assert guard.snapshot()["tracked_executions"] == 0


class TestLeaseBudget:
    def test_reserve_stops_exactly_at_the_budget(self, block42_topology):
        guard = FailureModelGuard(block42_topology)
        leases = [
            guard.reserve(
                guard.footprint_for(ChaosTarget.for_node(h, node_id=i), ImpactScope.NODE)
            )
            for i, h in enumerate(("h1", "h2", "h3", "h4"), start=1)
        ]
        assert [x is not None for x in leases] == [True, True, False, False]
        assert len(guard.snapshot()["impaired_racks"]) == 2

        assert guard.release(leases[1]) is True
        assert guard.reserve(
            guard.footprint_for(ChaosTarget.for_node("h3", node_id=3), ImpactScope.NODE)
        ), "a released domain frees room"

    def test_release_of_unknown_lease_is_a_noop(self, block42_topology):
        guard = FailureModelGuard(block42_topology)
        assert guard.release(None) is False and guard.release("never-issued") is False

    def test_datacenter_lease_is_the_sacrificial_realm(self, mirror3dc_topology):
        guard = FailureModelGuard(mirror3dc_topology)
        dc1 = guard.footprint_for(ChaosTarget.for_datacenter("dc1-a", "dc1"), ImpactScope.DATACENTER)
        assert guard.reserve(dc1), "a whole DC fits as the sacrificial realm"
        assert guard.reserve(
            guard.footprint_for(ChaosTarget.for_node("dc2-a", node_id=3), ImpactScope.NODE)
        ), "plus one domain elsewhere"
        assert guard.reserve(
            guard.footprint_for(ChaosTarget.for_node("dc3-a", node_id=5), ImpactScope.NODE)
        ) is None, "but not a second one"

    def test_budget_view_reports_impaired_identities(self, block42_topology):
        guard = FailureModelGuard(block42_topology)
        target = ChaosTarget.for_node("h1", node_id=1)
        lease = guard.reserve(
            guard.footprint_for(target, ImpactScope.NODE), identity_key=target.identity_key()
        )
        assert target.identity_key() in guard.budget_view().touched
        guard.release(lease)
        assert target.identity_key() not in guard.budget_view().touched


class TestSlotBudget:
    """Slots draw from their own 30% budget, not from the erasure budget."""

    def _slot_fp(self, guard, idx=1):
        return guard.footprint_for(ChaosTarget.for_slot("h1", slot_idx=idx), ImpactScope.SLOT)

    def test_slot_footprint_has_no_fail_domain(self, block42_topology):
        guard = FailureModelGuard(block42_topology, total_slots=10)
        # Even with a NODE scope, a SLOT-kind target draws from the slot budget.
        fp = guard.footprint_for(ChaosTarget.for_slot("h1", slot_idx=1), ImpactScope.NODE)
        assert fp.slots == 1 and fp.racks == frozenset()

    def test_reserve_caps_slots_at_thirty_percent(self, block42_topology):
        guard = FailureModelGuard(block42_topology, total_slots=10)  # max = 3
        assert guard.snapshot()["max_slots"] == 3
        assert all(guard.reserve(self._slot_fp(guard, i), identity_key=f"s{i}") for i in range(3))
        assert guard.reserve(self._slot_fp(guard, 3), identity_key="s3") is None
        assert guard.snapshot()["impaired_racks"] == [], "slots never spend a fail domain"

    def test_record_inject_charges_the_slot_budget(self, block42_topology):
        # The legacy loop and manual injects go through record_inject, not reserve.
        guard = FailureModelGuard(block42_topology, total_slots=10)
        for i in range(3):
            guard.record_inject(
                f"e{i}", ChaosTarget.for_slot("h1", slot_idx=i), ImpactScope.SLOT
            )
        assert guard.snapshot()["impaired_slots"] == 3
        assert not guard.budget_view().fits(self._slot_fp(guard, 9)), "budget spent"

    def test_slot_and_domain_budgets_are_independent(self, block42_topology):
        guard = FailureModelGuard(block42_topology, total_slots=3)  # max_slots = 1
        assert guard.reserve(self._slot_fp(guard), identity_key="s1")
        assert guard.reserve(self._slot_fp(guard, 2), identity_key="s2") is None
        assert guard.reserve(
            guard.footprint_for(ChaosTarget.for_node("h1", node_id=1), ImpactScope.NODE)
        ), "a domain fault still fits while the slot budget is full"

    def test_unknown_slot_count_never_blocks(self, block42_topology):
        guard = FailureModelGuard(block42_topology, total_slots=0)
        assert all(guard.reserve(self._slot_fp(guard, i), identity_key=f"s{i}") for i in range(5))


class TestSerialStaggeredPlanner:
    def _candidates(self, n: int = 4) -> list[ChaosTarget]:
        return [
            ChaosTarget.for_node(f"h{i}", node_id=i, ic_port=19000 + i) for i in range(1, n + 1)
        ]

    def test_dispatches_to_owner_hosts_with_their_own_ids(self):
        planner = SerialStaggeredInjectPlanner("SerialKillNodeNemesis", target_kind="node")
        by_node = {t.node_id: t for t in self._candidates()}

        cmds = planner.scheduled_tick(list(by_node.values()))
        assert 1 <= len(cmds) <= MAX_ENTITIES_PER_TICK
        assert len({c.target.identity_key() for c in cmds}) == len(cmds), "sampled without repeats"
        for cmd in cmds:
            chosen = by_node[cmd.target.node_id]
            assert cmd.host == chosen.host, "only the owner agent can kill the daemon"
            assert cmd.payload["node_id"] == chosen.node_id
            assert cmd.payload["node_ic_port"] == chosen.ic_port

    def test_kills_are_staggered_within_one_scenario(self):
        planner = SerialStaggeredInjectPlanner("SerialKillNodeNemesis", target_kind="node")
        for _ in range(50):  # sample until a tick picks more than one entity
            cmds = planner.scheduled_tick(self._candidates())
            if len(cmds) > 1:
                break
        assert len(cmds) > 1, "expected a multi-entity tick within 50 samples"
        delays = [c.payload["sleep_before"] for c in cmds]
        assert delays == [DEFAULT_SERIAL_STAGGER_SEC * i for i in range(len(cmds))], delays
        assert len({c.scenario_id for c in cmds}) == 1, "one tick is one scenario"
