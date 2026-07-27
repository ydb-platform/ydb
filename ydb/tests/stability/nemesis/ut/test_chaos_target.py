"""Unit tests for ChaosTarget / FailureModelGuard / serial planner core paths."""

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


def _write_topology_yaml(hosts: list[dict], erasure: str = "block-4-2") -> str:
    doc = {
        "static_erasure": erasure,
        "hosts": hosts,
    }
    fd, path = tempfile.mkstemp(suffix=".yaml")
    os.close(fd)
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


@pytest.fixture
def per_dc_rack_labels_topology():
    """mirror-3-dc where rack labels restart in every DC (``rack: '1'``, ``'2'`` per realm).

    This is how real ``cluster.yaml`` files label racks, and it is the case that collapsed into a
    single fail domain when the guard keyed impairments by the bare rack label.
    """
    hosts = [
        {"name": f"{dc}-{name}", "location": {"rack": rack, "data_center": dc}}
        for dc in ("dc1", "dc2", "dc3")
        for name, rack in (("a", "1"), ("b", "2"))
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

        # Joint filter accumulates admitted racks: block-4-2 budget is 2, so only first two.
        safe = guard.filter_safe([c1, c2, c3], ImpactScope.NODE)
        assert {t.host for t in safe} == {"h1", "h2"}, (
            "with empty impairments, block-4-2 joint filter must admit at most 2 domains; "
            f"got {[t.host for t in safe]}"
        )

        # Individually, each candidate is still tolerable against empty active set.
        for cand in (c1, c2, c3):
            alone = guard.filter_safe([cand], ImpactScope.NODE)
            assert alone == [cand], (
                f"single candidate {cand.host} must be safe alone; got {[t.host for t in alone]}"
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

        # Individually, either remaining realm's domain is still tolerable.
        for cand in (extra_ok, extra_other):
            alone = guard.filter_safe([cand], ImpactScope.NODE)
            assert alone == [cand], (
                f"with only dc1 lost, {cand.host} must be safe alone; "
                f"snapshot={guard.snapshot()}, alone={[t.host for t in alone]}"
            )

        # Jointly, accumulate admits only the first extra domain.
        safe = guard.filter_safe([extra_ok, extra_other], ImpactScope.NODE)
        assert [t.host for t in safe] == ["h3"], (
            "joint filter must not admit two extra domains after sacrificial realm; "
            f"snapshot={guard.snapshot()}, safe={[t.host for t in safe]}"
        )

        guard.record_inject("e-extra", extra_ok, ImpactScope.NODE, recovery_sec=None)
        safe_after = guard.filter_safe([extra_other], ImpactScope.NODE)
        assert safe_after == [], (
            "after dc1 + one domain in dc2, mirror-3-dc must reject a domain in a third realm "
            f"(h5/dc3); snapshot={guard.snapshot()}, safe_after={[t.host for t in safe_after]}"
        )

    def test_record_extract_fallback_matches_identity_not_rack(self, block42_topology):
        """Untracked extract must not drop unrelated impairments that share a rack."""
        guard = FailureModelGuard(block42_topology)
        # Two different node identities on the same host/rack.
        n1 = ChaosTarget.for_node("h1", node_id=1, ic_port=19001)
        n2 = ChaosTarget.for_node("h1", node_id=2, ic_port=19002)
        guard.record_inject("tracked", n1, ImpactScope.NODE, recovery_sec=None)
        # Simulate extract for an untracked execution of n2 (e.g. after restart).
        guard.record_extract("untracked-other", n2, ImpactScope.NODE)
        snap = guard.snapshot()
        assert "dc1/r1" in snap["impaired_racks"], (
            "extract fallback by identity must leave n1 impairment intact; "
            f"snapshot={snap}"
        )
        guard.record_extract("tracked", n1, ImpactScope.NODE)
        assert guard.snapshot()["impaired_racks"] == [], (
            f"tracked extract must release n1; snapshot={guard.snapshot()}"
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
        assert "dc1/r1" in guard.snapshot()["impaired_racks"], (
            f"host inject on h1 must mark fail domain dc1/r1 impaired; snapshot={guard.snapshot()}"
        )
        guard.record_extract("e1", t, ImpactScope.NODE)
        assert guard.snapshot()["impaired_racks"] == [], (
            f"extract must release impaired racks; snapshot={guard.snapshot()}"
        )


class TestFailureBudgetLease:
    """Per-candidate lease API (footprint_for / fits / reserve / release) for the new scheduler."""

    def test_footprint_for_node_is_single_rack(self, block42_topology):
        guard = FailureModelGuard(block42_topology)
        fp = guard.footprint_for(ChaosTarget.for_node("h1", node_id=1), ImpactScope.NODE)
        assert fp.racks == frozenset({"dc1/r1"}) and fp.slots == 0, (
            f"a node footprint must be exactly its host's fail domain, no slots; got {fp}"
        )

    def test_footprint_for_datacenter_covers_all_its_racks(self, mirror3dc_topology):
        guard = FailureModelGuard(mirror3dc_topology)
        fp = guard.footprint_for(
            ChaosTarget.for_datacenter("h1", "dc1"), ImpactScope.DATACENTER
        )
        assert fp.racks == frozenset({"dc1/r1", "dc1/r2"}), (
            f"a datacenter footprint must cover every rack in the DC; got {fp}"
        )

    def test_tablet_footprint_consumes_no_budget(self, block42_topology):
        guard = FailureModelGuard(block42_topology)
        fp = guard.footprint_for(ChaosTarget.for_tablet("h1", tablet_id=7), ImpactScope.NODE)
        assert not fp, f"a tablet footprint must be empty (no racks, no slots); got {fp}"
        lease = guard.reserve(fp)
        assert lease is not None, "reserving an empty footprint must always succeed"
        assert guard.snapshot()["impaired_racks"] == [], (
            f"reserving a tablet footprint must not impair any rack; snapshot={guard.snapshot()}"
        )

    def test_fits_is_read_only(self, block42_topology):
        guard = FailureModelGuard(block42_topology)
        fp = guard.footprint_for(ChaosTarget.for_node("h1", node_id=1), ImpactScope.NODE)
        assert guard.fits(fp) and guard.fits(fp), "fits must be repeatable and not consume budget"
        assert guard.snapshot()["impaired_racks"] == [], (
            f"fits must not mutate the impaired set; snapshot={guard.snapshot()}"
        )
        assert guard.reserve(fp) is not None, "budget untouched by fits, so reserve must still fit"

    def test_reserve_release_block42_budget(self, block42_topology):
        guard = FailureModelGuard(block42_topology)
        fp1 = guard.footprint_for(ChaosTarget.for_node("h1", node_id=1), ImpactScope.NODE)
        fp2 = guard.footprint_for(ChaosTarget.for_node("h2", node_id=2), ImpactScope.NODE)
        fp3 = guard.footprint_for(ChaosTarget.for_node("h3", node_id=3), ImpactScope.NODE)

        l1 = guard.reserve(fp1)
        l2 = guard.reserve(fp2)
        assert l1 and l2, "block-4-2 must admit two distinct fail domains"

        assert guard.reserve(fp3) is None, (
            "a third distinct domain must exceed the block-4-2 budget; "
            f"snapshot={guard.snapshot()}"
        )
        assert not guard.fits(fp3), "fits must agree with reserve that a third domain does not fit"

        assert guard.release(l2) is True, "releasing an active lease must report success"
        l3 = guard.reserve(fp3)
        assert l3 is not None, (
            f"after releasing one domain a third must fit again; snapshot={guard.snapshot()}"
        )

    def test_reserve_is_atomic_never_exceeds_budget(self, block42_topology):
        """Sequential reservations of every rack must stop exactly at the budget."""
        guard = FailureModelGuard(block42_topology)
        leases = []
        for host, rack_node in (("h1", 1), ("h2", 2), ("h3", 3), ("h4", 4)):
            fp = guard.footprint_for(
                ChaosTarget.for_node(host, node_id=rack_node), ImpactScope.NODE
            )
            leases.append(guard.reserve(fp))
        granted = [x for x in leases if x is not None]
        assert len(granted) == 2, (
            f"block-4-2 must grant exactly 2 leases across 4 distinct racks; granted={leases}"
        )
        assert len(guard.snapshot()["impaired_racks"]) == 2, (
            f"impaired set must never exceed the budget; snapshot={guard.snapshot()}"
        )

    def test_datacenter_footprint_mirror3dc_sacrificial_realm(self, mirror3dc_topology):
        guard = FailureModelGuard(mirror3dc_topology)
        dc1 = guard.footprint_for(
            ChaosTarget.for_datacenter("h1", "dc1"), ImpactScope.DATACENTER
        )
        node_dc2 = guard.footprint_for(ChaosTarget.for_node("h3", node_id=3), ImpactScope.NODE)
        node_dc3 = guard.footprint_for(ChaosTarget.for_node("h5", node_id=5), ImpactScope.NODE)

        ldc = guard.reserve(dc1)
        assert ldc is not None, "a whole DC must fit as the sacrificial realm on an empty budget"

        lextra = guard.reserve(node_dc2)
        assert lextra is not None, (
            f"one extra domain in another realm must fit after sacrificing dc1; "
            f"snapshot={guard.snapshot()}"
        )

        assert guard.reserve(node_dc3) is None, (
            "a domain in a third realm must be rejected (1 realm + 1 domain budget); "
            f"snapshot={guard.snapshot()}"
        )

        assert guard.release(ldc) is True
        lthird = guard.reserve(node_dc3)
        assert lthird is not None, (
            f"after releasing dc1, single domains in dc2 and dc3 must both fit; "
            f"snapshot={guard.snapshot()}"
        )

    def test_reserve_records_identity_for_active_identities(self, block42_topology):
        guard = FailureModelGuard(block42_topology)
        target = ChaosTarget.for_node("h1", node_id=1)
        fp = guard.footprint_for(target, ImpactScope.NODE)
        assert guard.active_identities() == set(), "no reservations means no active identities"
        lease = guard.reserve(fp, identity_key=target.identity_key())
        assert lease is not None
        assert target.identity_key() in guard.active_identities(), (
            "a reserved target's identity must be reported so schedulers skip re-hitting it; "
            f"active={guard.active_identities()}"
        )
        assert guard.release(lease) is True
        assert target.identity_key() not in guard.active_identities(), (
            "releasing the lease must drop its identity from the active set"
        )

    def test_release_of_disabled_or_unknown_lease_is_noop(self, block42_topology):
        guard = FailureModelGuard(block42_topology)
        assert guard.release(None) is False, "releasing a None lease must be a no-op"
        assert guard.release("never-issued") is False, (
            "releasing an unknown lease id must be a no-op, not raise"
        )


class TestSlotBudget:
    """Slots (dynamic nodes) draw from a separate 30% budget, not the erasure/rack budget."""

    def _slot_fp(self, guard):
        return guard.footprint_for(
            ChaosTarget.for_slot("h1", slot_idx=1), ImpactScope.SLOT
        )

    def test_slot_footprint_is_slot_not_rack(self, block42_topology):
        guard = FailureModelGuard(block42_topology, total_slots=10)
        fp = self._slot_fp(guard)
        assert fp.slots == 1 and fp.racks == frozenset(), (
            f"a slot kill must consume one slot and zero racks; got {fp}"
        )

    def test_slot_kind_target_never_touches_racks(self, block42_topology):
        # Even annotated NODE scope, a SLOT-kind target must not eat a rack fail-domain.
        guard = FailureModelGuard(block42_topology, total_slots=10)
        fp = guard.footprint_for(ChaosTarget.for_slot("h1", slot_idx=1), ImpactScope.NODE)
        assert fp.slots == 1 and fp.racks == frozenset(), (
            f"a SLOT-kind target must draw from the slot budget regardless of scope; got {fp}"
        )

    def test_slot_budget_caps_at_thirty_percent(self, block42_topology):
        guard = FailureModelGuard(block42_topology, total_slots=10)  # max = floor(0.3*10) = 3
        assert guard.snapshot()["max_slots"] == 3
        leases = [
            guard.reserve(
                guard.footprint_for(ChaosTarget.for_slot("h1", slot_idx=i), ImpactScope.SLOT),
                identity_key=f"slot:{i}",
            )
            for i in range(3)
        ]
        assert all(leases), "the first 30% of slots must all reserve"
        assert guard.snapshot()["impaired_slots"] == 3
        overflow = guard.reserve(
            guard.footprint_for(ChaosTarget.for_slot("h1", slot_idx=3), ImpactScope.SLOT),
            identity_key="slot:3",
        )
        assert overflow is None, "a 4th slot must exceed the 30% budget"
        assert guard.snapshot()["impaired_racks"] == [], (
            "slot reservations must never impair a rack fail-domain"
        )

    def test_slot_and_rack_budgets_are_independent(self, block42_topology):
        # Fill the slot budget, then a node (rack) fault must still fit — dimensions don't cross.
        guard = FailureModelGuard(block42_topology, total_slots=3)  # max_slots = floor(0.9)->1
        assert guard.snapshot()["max_slots"] == 1
        slot = guard.reserve(self._slot_fp(guard), identity_key="slot:1")
        assert slot is not None
        assert guard.reserve(self._slot_fp(guard), identity_key="slot:2") is None, (
            "the slot budget is full at 1, so a second slot must be rejected"
        )
        node_fp = guard.footprint_for(ChaosTarget.for_node("h1", node_id=1), ImpactScope.NODE)
        assert guard.fits(node_fp) and guard.reserve(node_fp) is not None, (
            "a rack fault must still fit while the slot budget is exhausted (independent budgets)"
        )

    def test_record_inject_charges_the_slot_budget(self, block42_topology):
        # The legacy schedule loop and manual injects go through record_inject, not reserve —
        # they must spend the same slot budget, otherwise the two paths disagree.
        guard = FailureModelGuard(block42_topology, total_slots=10)  # max = 3
        for i in range(3):
            guard.record_inject(
                f"exec{i}",
                ChaosTarget.for_slot("h1", slot_idx=i),
                ImpactScope.SLOT,
                recovery_sec=None,
            )
        snap = guard.snapshot()
        assert snap["impaired_slots"] == 3, f"slot injects must charge the slot budget; got {snap}"
        assert snap["impaired_racks"] == [], "a slot kill still must not spend a fail domain"
        assert not guard.fits(self._slot_fp(guard)), (
            "with the slot budget spent by record_inject, a further slot must not fit"
        )

    def test_record_inject_of_a_tablet_records_nothing(self, block42_topology):
        guard = FailureModelGuard(block42_topology, total_slots=10)
        guard.record_inject(
            "t1", ChaosTarget.for_tablet("h1", tablet_id=1), ImpactScope.NODE, recovery_sec=None
        )
        snap = guard.snapshot()
        assert snap["tracked_executions"] == 0 and snap["impaired_slots"] == 0, (
            f"a tablet fault consumes neither dimension; got {snap}"
        )

    def test_slot_budget_fails_open_when_no_slots_known(self, block42_topology):
        guard = FailureModelGuard(block42_topology, total_slots=0)
        assert guard.snapshot()["max_slots"] == 0
        # With no slot total, the slot dimension never blocks (fail-open).
        for i in range(5):
            fp = guard.footprint_for(ChaosTarget.for_slot("h1", slot_idx=i), ImpactScope.SLOT)
            assert guard.reserve(fp, identity_key=f"slot:{i}") is not None, (
                "unknown slot total must fail open, never blocking slot chaos"
            )


class TestFailureModelIsMandatory:
    """An unusable cluster.yaml must raise, not degrade into a guard that permits everything.

    ``app.create_app`` lets this exception kill the orchestrator: unbounded chaos destroys data and
    turns every stability failure into noise, so no failure model means no nemesis.
    """

    def test_missing_path_raises(self):
        for path in ("", None):
            with pytest.raises(FailureModelConfigError):
                ClusterTopologyModel(path)

    def test_missing_file_raises(self):
        with pytest.raises(FailureModelConfigError, match="not found"):
            ClusterTopologyModel("/nonexistent/cluster.yaml")

    def test_unparsable_yaml_raises(self, tmp_path):
        bad = tmp_path / "cluster.yaml"
        bad.write_text("static_erasure: [unclosed\n", encoding="utf-8")
        with pytest.raises(FailureModelConfigError, match="cannot parse"):
            ClusterTopologyModel(str(bad))

    def test_non_mapping_document_raises(self, tmp_path):
        bad = tmp_path / "cluster.yaml"
        bad.write_text("- just\n- a\n- list\n", encoding="utf-8")
        with pytest.raises(FailureModelConfigError, match="YAML mapping"):
            ClusterTopologyModel(str(bad))

    def test_unknown_erasure_raises(self):
        hosts = [{"name": "h1", "location": {"rack": "r1", "data_center": "dc1"}}]
        with pytest.raises(FailureModelConfigError, match="static_erasure"):
            ClusterTopologyModel(_write_topology_yaml(hosts, "no-such-erasure"))

    def test_no_hosts_raises(self):
        with pytest.raises(FailureModelConfigError, match="hosts"):
            ClusterTopologyModel(_write_topology_yaml([], "block-4-2"))

    def test_host_without_rack_raises(self):
        hosts = [
            {"name": "h1", "location": {"rack": "r1", "data_center": "dc1"}},
            {"name": "h2", "location": {"data_center": "dc1"}},
        ]
        with pytest.raises(FailureModelConfigError, match="location.rack"):
            ClusterTopologyModel(_write_topology_yaml(hosts, "block-4-2"))

    def test_host_without_location_raises(self):
        with pytest.raises(FailureModelConfigError, match="location"):
            ClusterTopologyModel(_write_topology_yaml([{"name": "h1"}], "block-4-2"))

    def test_mirror3dc_requires_datacenter(self):
        # Realms may be sacrificed whole, so a host with an unknown realm is not decidable.
        hosts = [{"name": f"h{i}", "location": {"rack": f"r{i}"}} for i in (1, 2, 3)]
        with pytest.raises(FailureModelConfigError, match="data_center"):
            ClusterTopologyModel(_write_topology_yaml(hosts, "mirror-3-dc"))

    def test_block42_without_datacenter_is_accepted(self):
        # block-4-2 counts domains regardless of realm, so data_center is optional there.
        hosts = [{"name": f"h{i}", "location": {"rack": f"r{i}"}} for i in (1, 2, 3)]
        topology = ClusterTopologyModel(_write_topology_yaml(hosts, "block-4-2"))
        assert topology.guards
        assert topology.domain_of("h1") == fail_domain_key(None, "r1")

    def test_erasure_none_parses_and_forbids_everything(self):
        hosts = [{"name": "h1", "location": {"rack": "r1", "data_center": "dc1"}}]
        guard = FailureModelGuard(ClusterTopologyModel(_write_topology_yaml(hosts, "none")))
        assert guard.enabled, "static_erasure=none is a valid model (zero tolerance), not an error"
        fp = guard.footprint_for(ChaosTarget.for_node("h1", node_id=1), ImpactScope.NODE)
        assert not guard.fits(fp) and guard.reserve(fp) is None, (
            "with no redundancy the guard must refuse every fault that touches a fail domain"
        )


class TestFailDomainKeying:
    """Rack labels repeat per datacenter, so a fail domain must be keyed by (dc, rack).

    Keying by the bare rack label collapsed rack ``1`` of every DC into one domain, and the guard
    then admitted a fault in every realm at once — exactly what the failure model forbids.
    """

    def test_key_is_namespaced_by_datacenter(self):
        assert fail_domain_key("dc1", "1") != fail_domain_key("dc2", "1"), (
            "rack '1' in two datacenters must be two distinct fail domains"
        )
        assert fail_domain_key(None, "1") == "?/1", (
            f"a host without data_center must still get a stable key; got {fail_domain_key(None, '1')!r}"
        )

    def test_same_rack_label_per_dc_are_distinct_domains(self, per_dc_rack_labels_topology):
        topology = per_dc_rack_labels_topology
        assert topology.domain_of("dc1-a") != topology.domain_of("dc2-a"), (
            "hosts in different DCs sharing rack label '1' must map to different fail domains; "
            f"dc1-a={topology.domain_of('dc1-a')!r}, dc2-a={topology.domain_of('dc2-a')!r}"
        )
        for dc in ("dc1", "dc2", "dc3"):
            assert topology.domains_in_dc(dc) == {
                fail_domain_key(dc, "1"),
                fail_domain_key(dc, "2"),
            }, (
                f"every realm must keep its own two domains; {dc} -> {topology.domains_in_dc(dc)}"
            )

    def test_mirror3dc_rejects_a_fault_in_a_third_realm(self, per_dc_rack_labels_topology):
        guard = FailureModelGuard(per_dc_rack_labels_topology)
        assert guard.enabled

        leases = []
        for host in ("dc1-a", "dc2-a"):  # rack '1' in two different DCs
            target = ChaosTarget.for_node(host, node_id=1)
            fp = guard.footprint_for(target, ImpactScope.NODE)
            leases.append(guard.reserve(fp, identity_key=target.identity_key()))
        assert all(leases), (
            f"one domain per realm in two realms must fit mirror-3-dc; snapshot={guard.snapshot()}"
        )
        assert len(guard.snapshot()["impaired_racks"]) == 2, (
            "the two same-labelled racks must be counted as two domains, not one; "
            f"snapshot={guard.snapshot()}"
        )

        third = ChaosTarget.for_node("dc3-a", node_id=1)  # rack '1' again, third realm
        third_fp = guard.footprint_for(third, ImpactScope.NODE)
        assert not guard.fits(third_fp), (
            "mirror-3-dc tolerates 1 realm + 1 domain, so a third realm must not fit; "
            f"snapshot={guard.snapshot()}"
        )
        assert guard.reserve(third_fp, identity_key=third.identity_key()) is None, (
            f"reserve must refuse the third realm; snapshot={guard.snapshot()}"
        )
        assert guard.filter_safe([third], ImpactScope.NODE) == [], (
            f"filter_safe must agree with reserve; snapshot={guard.snapshot()}"
        )

    def test_block42_counts_same_label_racks_separately(self):
        # Two DCs × rack '1'/'2'; block-4-2 tolerates 2 domains total, whatever their labels.
        hosts = [
            {"name": f"{dc}-{rack}", "location": {"rack": rack, "data_center": dc}}
            for dc in ("dc1", "dc2")
            for rack in ("1", "2")
        ]
        guard = FailureModelGuard(ClusterTopologyModel(_write_topology_yaml(hosts, "block-4-2")))
        granted = []
        for host in ("dc1-1", "dc2-1", "dc1-2"):
            target = ChaosTarget.for_node(host, node_id=1)
            granted.append(
                guard.reserve(
                    guard.footprint_for(target, ImpactScope.NODE),
                    identity_key=target.identity_key(),
                )
            )
        assert [g is not None for g in granted] == [True, True, False], (
            "rack '1' of dc1 and dc2 are two domains, so the third fault must exceed block-4-2; "
            f"granted={granted}, snapshot={guard.snapshot()}"
        )

    def test_datacenter_footprint_covers_only_its_own_realm(self, per_dc_rack_labels_topology):
        guard = FailureModelGuard(per_dc_rack_labels_topology)
        fp = guard.footprint_for(ChaosTarget.for_datacenter("dc1-a", "dc1"), ImpactScope.DATACENTER)
        assert fp.racks == {fail_domain_key("dc1", "1"), fail_domain_key("dc1", "2")}, (
            "a DC footprint must be that DC's own domains — not a synthetic single-host key, and "
            f"not another realm's racks; got {set(fp.racks)}"
        )

    def test_unknown_hosts_get_realm_aware_synthetic_domains(self, per_dc_rack_labels_topology):
        # A host that cluster.yaml has no rack for (e.g. an agent outside the config) still gets a
        # realm-namespaced key, so two such hosts are not merged into one sacrificial realm.
        guard = FailureModelGuard(per_dc_rack_labels_topology)
        keys = {
            host: set(
                guard.footprint_for(ChaosTarget.for_host(host), ImpactScope.NODE).racks
            ).pop()
            for host in ("ghost-a", "ghost-b")
        }
        assert keys["ghost-a"] != keys["ghost-b"], (
            f"unknown hosts must not share a fail domain; got {keys}"
        )
        assert all(k.startswith("__host__:") for k in keys.values()), keys


class TestSyntheticDomainRealms:
    """Hosts whose realm *is* known but whose rack is not must be attributed to that realm."""

    def test_mirror3dc_counts_unknown_rack_hosts_per_realm(self):
        # h1/h2 are in cluster.yaml (dc1, dc2); the guard is asked about hosts it has topology for
        # but through a scope that has no rack — the synthetic key must still carry the realm.
        hosts = [
            {"name": "h1", "location": {"rack": "r1", "data_center": "dc1"}},
            {"name": "h2", "location": {"rack": "r2", "data_center": "dc2"}},
            {"name": "h3", "location": {"rack": "r3", "data_center": "dc3"}},
        ]
        guard = FailureModelGuard(ClusterTopologyModel(_write_topology_yaml(hosts, "mirror-3-dc")))
        domains = {
            guard._synthetic_key("h1"),
            guard._synthetic_key("h2"),
        }
        assert domains == {"__host__:dc1/h1", "__host__:dc2/h2"}, (
            f"synthetic keys must be namespaced by realm; got {domains}"
        )
        # One synthetic domain per realm in two realms is still 1 sacrificial realm + 1 domain.
        assert guard._is_tolerable(domains), f"two realms must be tolerable; domains={domains}"
        assert not guard._is_tolerable(domains | {guard._synthetic_key("h3")}), (
            "a third realm must not be tolerable — the old un-namespaced key made all three look "
            "like one realm"
        )


class TestSerialStaggeredPlanner:
    def _candidates(self, n: int = 4) -> list[ChaosTarget]:
        return [
            ChaosTarget.for_node(f"h{i}", node_id=i, ic_port=19000 + i)
            for i in range(1, n + 1)
        ]

    def test_dispatches_only_to_owner_hosts(self):
        planner = SerialStaggeredInjectPlanner("SerialKillNodeNemesis", target_kind="node")
        candidates = self._candidates()
        by_node = {t.node_id: t for t in candidates}

        cmds = planner.scheduled_tick(candidates)
        assert 1 <= len(cmds) <= MAX_ENTITIES_PER_TICK, (
            f"a serial tick must kill between 1 and {MAX_ENTITIES_PER_TICK} entities; got {len(cmds)}"
        )
        assert len({c.target.identity_key() for c in cmds}) == len(cmds), (
            f"entities must be sampled without repetition; got {[c.target.to_dict() for c in cmds]}"
        )
        for cmd in cmds:
            chosen = by_node.get(cmd.target.node_id)
            assert chosen is not None, f"node_id must come from the candidates; got {cmd.target!r}"
            assert cmd.host == chosen.host, (
                f"dispatch must go to the owner of the chosen node, not a random host; "
                f"host={cmd.host!r}, owner={chosen.host!r}"
            )
            assert cmd.payload.get("node_id") == cmd.target.node_id, (
                f"payload.node_id must match ChaosTarget.node_id; payload={cmd.payload}"
            )
            assert cmd.payload.get("node_ic_port") == chosen.ic_port, (
                f"payload must carry the entity's own ic_port; payload={cmd.payload}"
            )

    def test_kills_are_staggered_in_time_within_one_scenario(self):
        planner = SerialStaggeredInjectPlanner(
            "SerialKillNodeNemesis", target_kind="node", stagger_sec=7.5
        )
        # Sample until a tick picks more than one entity, so the stagger is observable.
        for _ in range(50):
            cmds = planner.scheduled_tick(self._candidates())
            if len(cmds) > 1:
                break
        assert len(cmds) > 1, "expected at least one multi-entity tick out of 50 samples"

        delays = [c.payload["sleep_before"] for c in cmds]
        assert delays == [7.5 * i for i in range(len(cmds))], (
            f"the i-th kill must wait i * stagger_sec so kills are serial, not simultaneous; "
            f"got {delays}"
        )
        assert len({c.scenario_id for c in cmds}) == 1, (
            "one staggered tick is one scenario, so its commands must share a scenario id"
        )

    def test_default_stagger_is_used_when_not_overridden(self):
        planner = SerialStaggeredInjectPlanner("SerialKillNodeNemesis", target_kind="node")
        for _ in range(50):
            cmds = planner.scheduled_tick(self._candidates())
            if len(cmds) > 1:
                break
        assert len(cmds) > 1, "expected at least one multi-entity tick out of 50 samples"
        assert cmds[1].payload["sleep_before"] == DEFAULT_SERIAL_STAGGER_SEC, (
            f"the default stagger must be {DEFAULT_SERIAL_STAGGER_SEC}s; got {cmds[1].payload}"
        )
