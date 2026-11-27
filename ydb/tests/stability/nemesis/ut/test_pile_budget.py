"""Independent pile budgets, including multiple YDB processes on one physical host."""

from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
import yaml

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import ChaosTarget
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.cluster_inventory import NodeInfo, SlotInfo
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    ClusterTopologyModel, FailureModelConfigError, FailureModelGuard, Footprint, ImpactScope,
)


def cluster(tmp_path, *, local=False, erasure="block-4-2", slots=(10, 20)):
    hosts, nodes, dynamic = [], {}, {}
    for pile in (0, 1):
        for index in range(3):
            node_id = pile * 10 + index + 1
            host = f"p{pile}-n{index}"
            dc = f"dc{index}" if erasure == "mirror-3-dc" else "dc"
            hosts.append({"name": host, "location": {
                "rack": str(index), "data_center": dc, "bridge_pile_name": f"p{pile}",
            }})
            nodes[node_id] = NodeInfo(node_id, "localhost" if local else host, 19000 + node_id,
                                      str(index), dc, pile)
        for index in range(slots[pile]):
            idx = pile * 100 + index
            dynamic[idx] = SlotInfo(idx, "localhost" if local else f"p{pile}-n0", 20000 + idx,
                                    1000 + idx, "0", "dc0" if erasure == "mirror-3-dc" else "dc", pile)
    doc = {"config": {"erasure": erasure, "bridge_config": {"piles": [{"name": "p0"}, {"name": "p1"}]},
                      "hosts": hosts}}
    path = tmp_path / "cluster.yaml"
    path.write_text(yaml.safe_dump(doc))
    topology = ClusterTopologyModel(str(path))
    inventory = SimpleNamespace(nodes=nodes, slots=dynamic)
    guard = FailureModelGuard(topology, total_slots=sum(slots), inventory=inventory)
    return guard, inventory, doc, path


def node_target(inventory, pile, index):
    n = inventory.nodes[pile * 10 + index + 1]
    return ChaosTarget.for_node(n.host, node_id=n.node_id, ic_port=n.ic_port)


def slot_target(inventory, pile, index):
    s = inventory.slots[pile * 100 + index]
    return ChaosTarget.for_slot(s.host, slot_idx=s.slot_idx, node_id=s.node_id, ic_port=s.ic_port)


def reserve(guard, target, scope):
    return guard.reserve(guard.footprint_for(target, scope), target.identity_key(), target=target)


@pytest.mark.parametrize("local", [False, True])
@pytest.mark.parametrize("erasure", ["block-4-2", "mirror-3-dc"])
def test_storage_limits_and_release_are_independent(tmp_path, local, erasure):
    guard, inventory, _, _ = cluster(tmp_path, local=local, erasure=erasure)
    leases = {}
    for pile in (0, 1):
        for index in (0, 1):
            n = node_target(inventory, pile, index)
            disk = ChaosTarget.for_disk(n.host, node_id=n.node_id, ic_port=n.ic_port, disk_path=f"/pdisk/{n.node_id}")
            leases[pile, index] = reserve(guard, disk, ImpactScope.DISK)
            assert leases[pile, index]
        third = node_target(inventory, pile, 2)
        assert reserve(guard, third, ImpactScope.NODE) is None
        assert not guard.budget_view().fits(guard.footprint_for(third, ImpactScope.NODE))
    snapshot = guard.snapshot()
    assert len(snapshot["impaired_racks"]) == 4
    assert all(len(p["impaired_racks"]) == 2 for p in snapshot["piles"].values())
    guard.release(leases[0, 0])
    assert reserve(guard, node_target(inventory, 0, 2), ImpactScope.NODE)
    assert reserve(guard, node_target(inventory, 1, 2), ImpactScope.NODE) is None


@pytest.mark.parametrize("local", [False, True])
def test_slot_budgets_use_each_piles_population(tmp_path, local):
    guard, inventory, _, _ = cluster(tmp_path, local=local)
    leases = {}
    for pile, limit in ((0, 3), (1, 6)):
        leases[pile] = [reserve(guard, slot_target(inventory, pile, i), ImpactScope.SLOT) for i in range(limit)]
        assert all(leases[pile])
        assert reserve(guard, slot_target(inventory, pile, limit), ImpactScope.SLOT) is None
    snapshot = guard.snapshot()
    assert snapshot["impaired_slots"] == snapshot["max_slots"] == 9
    assert snapshot["piles"]["0"]["max_slots"] == 3
    assert snapshot["piles"]["1"]["max_slots"] == 6
    assert snapshot["impaired_racks"] == []
    guard.release(leases[0][0])
    assert reserve(guard, slot_target(inventory, 0, 3), ImpactScope.SLOT)
    assert reserve(guard, slot_target(inventory, 1, 6), ImpactScope.SLOT) is None
    # Slots do not consume the independent storage budget.
    assert reserve(guard, node_target(inventory, 1, 0), ImpactScope.NODE)


def test_filter_and_reserve_do_not_borrow_slots_from_other_pile(tmp_path):
    guard, inventory, _, _ = cluster(tmp_path)
    targets = [slot_target(inventory, p, i) for p in (0, 1) for i in range(8)]
    assert len(guard.filter_safe(targets, ImpactScope.SLOT)) == 9
    assert len(guard.filter_safe(targets, ImpactScope.SLOT, jointly=False)) == 16
    for i in range(3):
        assert reserve(guard, slot_target(inventory, 0, i), ImpactScope.SLOT)
    assert guard.filter_safe([slot_target(inventory, 0, 3)], ImpactScope.SLOT) == []
    assert guard.filter_safe([slot_target(inventory, 1, 0)], ImpactScope.SLOT)
    assert guard.reserve(Footprint(slots=1)) is None  # no silent global/unknown budget


def test_reservation_is_atomic_across_threads(tmp_path):
    guard, inventory, _, _ = cluster(tmp_path, slots=(10, 10))
    targets = [slot_target(inventory, p, i) for p in (0, 1) for i in range(10)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        leases = list(pool.map(lambda target: reserve(guard, target, ImpactScope.SLOT), targets))
    assert sum(lease is not None for lease in leases) == 6
    assert [p["impaired_slots"] for p in guard.snapshot()["piles"].values()] == [3, 3]


def test_group_and_physical_host_impacts_include_all_resources(tmp_path):
    guard, inventory, _, _ = cluster(tmp_path, local=True)
    fp = guard.footprint_for(ChaosTarget.for_host("localhost"), ImpactScope.NODE)
    assert len(fp.racks) == 6 and dict(fp.slots_by_pile) == {0: 10, 1: 20}
    assert guard.reserve(fp) is None
    pile_fp = guard.footprint_for(ChaosTarget.for_pile("localhost", 1), ImpactScope.PILE)
    assert len(pile_fp.racks) == 3 and dict(pile_fp.slots_by_pile) == {1: 20}
    # Repeated DC labels span both piles, rather than silently selecting the agent's first node.
    dc_fp = guard.footprint_for(ChaosTarget.for_datacenter("localhost", "dc"), ImpactScope.DATACENTER)
    assert dc_fp == fp
    # Resolve a concrete process independently of its shared transport address.
    single = guard.footprint_for(node_target(inventory, 1, 0), ImpactScope.NODE)
    assert single.racks == frozenset({"pile:1/dc/0"}) and single.slots == 0


def test_failed_combined_reservation_leaves_both_budgets_unchanged(tmp_path):
    guard, inventory, _, _ = cluster(tmp_path)
    for i in range(3):
        reserve(guard, slot_target(inventory, 0, i), ImpactScope.SLOT)
    before = guard.snapshot()
    combined = Footprint(slots=2, slots_by_pile=((0, 1), (1, 1)))
    assert not guard.budget_view().fits(combined)
    assert guard.reserve(combined) is None
    assert guard.snapshot() == before


def test_record_and_extract_keep_pile_association(tmp_path):
    guard, inventory, _, _ = cluster(tmp_path)
    targets = [slot_target(inventory, p, 0) for p in (0, 1)]
    for p, target in enumerate(targets):
        guard.record_inject(str(p), target, ImpactScope.SLOT)
    guard.record_extract("new-extract-id", targets[0], ImpactScope.SLOT)
    assert [p["impaired_slots"] for p in guard.snapshot()["piles"].values()] == [0, 1]


@pytest.mark.parametrize("missing", [None, "typo"])
def test_bridge_host_requires_known_pile(tmp_path, missing):
    _, _, doc, path = cluster(tmp_path)
    doc["config"]["hosts"][0]["location"]["bridge_pile_name"] = missing
    path.write_text(yaml.safe_dump(doc))
    with pytest.raises(FailureModelConfigError, match="bridge_pile_name"):
        ClusterTopologyModel(str(path))


def test_bridge_rejects_unknown_process_and_missing_slot_inventory(tmp_path):
    guard, _, _, path = cluster(tmp_path, local=True)
    with pytest.raises(ValueError, match="unique resource"):
        guard.footprint_for(ChaosTarget.for_node("localhost", node_id=999), ImpactScope.NODE)
    with pytest.raises(FailureModelConfigError, match="per-slot inventory"):
        FailureModelGuard(ClusterTopologyModel(str(path)), total_slots=30)
