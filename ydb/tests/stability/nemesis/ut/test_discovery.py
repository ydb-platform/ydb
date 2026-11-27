"""Discovery preserves expected membership and lease ownership across outages."""
from unittest.mock import Mock

import pytest

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.discovery import ClusterDiscovery
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import FailureModelGuard
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.fault_engine import RecoveryUnavailable


def responses(count=2):
    config = {"SelfManagementConfig": {"ErasureSpecies": "block-4-2"},
              "BridgeConfig": {"Piles": [{"Name": "a"}, {"Name": "b"}]},
              "NameserviceConfig": {"Node": [{"NodeId": i, "Host": "localhost"} for i in range(1, count + 1)]}}
    nodes = [{"NodeId": i, "SystemState": {
        "Host": "physical-host", "Location": {"Rack": str(i), "DataCenter": "dc", "BridgePileName": "a" if i % 2 else "b"},
        "Endpoints": [{"Name": name, "Address": f":{port + i}"}
                      for name, port in [("ic", 10000), ("grpc", 11000), ("http-mon", 12000)]],
    }} for i in range(1, count + 1)]
    return config, {"TotalNodes": count, "Nodes": nodes}


def test_discover_refresh_and_preserve_missing_node():
    d = ClusterDiscovery("http://localhost:12001")
    c, v = responses()
    first = d._parse(c, v, [], d.http[0])
    d.publish(first)
    assert d.grpc == ("grpc://localhost:11001", "grpc://localhost:11002")
    assert d.node_piles == {1: 0, 2: 1}
    c, v = responses(4)
    second = d._parse(c, v, [], d.http[0])
    assert len(second.inventory.nodes) == 4
    guard = FailureModelGuard(first.topology, inventory=first.inventory)
    guard.reconfigure(FailureModelGuard(second.topology, inventory=second.inventory))
    assert len(guard._placement.nodes) == 4
    d.publish(second)
    v["Nodes"][0] = {"NodeId": 1, "Disconnected": True}
    third = d._parse(c, v, [], d.http[0])
    assert len(third.inventory.nodes) == 4
    assert "http://localhost:12001" not in third.http
    guard._impairments.append(object())
    with pytest.raises(RuntimeError, match="active leases"):
        guard.reconfigure(FailureModelGuard(first.topology, inventory=first.inventory))


def test_failover_and_stale_discovery():
    d = ClusterDiscovery("http://localhost:12001", max_age=10)
    first = d._parse(*responses(), [], d.http[0])
    d.publish(first)
    config, nodes = responses()
    d._get = Mock(side_effect=[ValueError("offline"), Mock(json=lambda: config), Mock(json=lambda: nodes)])
    assert d.fetch([]).grpc == first.grpc
    d.last_observation -= 11
    with pytest.raises(RecoveryUnavailable):
        d.check()
    assert d.grpc == first.grpc


@pytest.mark.parametrize("fault", ["pagination", "pile", "erasure"])
def test_reject_incomplete_topology(fault):
    d = ClusterDiscovery("http://localhost:12001")
    c, v = responses()
    if fault == "pagination":
        v["TotalNodes"] = 3
    elif fault == "pile":
        v["Nodes"][0]["SystemState"]["Location"].pop("BridgePileName")
    else:
        c.pop("SelfManagementConfig")
    with pytest.raises(ValueError):
        d._parse(c, v, [], d.http[0])


def test_dynamic_slots_refresh_budgets_independently():
    d = ClusterDiscovery("http://localhost:12001")
    first = d._parse(*responses(), [], d.http[0])
    d.publish(first)
    c, v = responses(4)
    c["NameserviceConfig"]["Node"] = c["NameserviceConfig"]["Node"][:2]
    expanded = d._parse(c, v, [], d.http[0])
    guard = FailureModelGuard(first.topology, inventory=first.inventory)
    guard.reconfigure(FailureModelGuard(expanded.topology, inventory=expanded.inventory))
    assert guard._pile_totals == {0: 1, 1: 1}
    assert guard._pile_limits == {0: 1, 1: 1}
    d.publish(expanded)
    v["Nodes"][2]["NodeId"] = 1003
    restarted = d._parse(c, v, [], d.http[0])
    assert {s.node_id for s in restarted.inventory.slots.values()} == {1003, 4}
    assert len(restarted.inventory.slots) == 2


@pytest.mark.parametrize("address", ["localhost", "grpc://localhost:2135", "grpcs://localhost:3135"])
def test_endpoint_healthcheck_preserves_host_identity(address):
    from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.hc_model import build_snapshot
    snapshot = build_snapshot({address: {"self_check_result": "GOOD"}},
                              now=1, last_update=1, max_age_sec=10)
    assert snapshot.fresh and snapshot.answering == {"localhost"}


def test_reorder_shrink_and_fresh_observations():
    d = ClusterDiscovery("http://localhost:12001")
    c, v = responses(22)
    c["NameserviceConfig"]["Node"] = c["NameserviceConfig"]["Node"][:2]
    first = d._parse(c, v, [], d.http[0])
    d.publish(first)
    c["BridgeConfig"]["Piles"].reverse()
    reordered = d._parse(c, v, [], d.http[0])
    assert reordered.node_piles == first.node_piles
    d.last_observation -= 100
    from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.fault_engine import FaultEngine
    engine = FaultEngine(guard=None, inventory=None, plan_inject=None, dispatch=None,
                         recovery_probe=Mock(pending=Mock(return_value=[object()])))
    engine.refresh_topology(d, reordered, controller=None, service=None)
    d.check()
    assert d.generation == 1
    v["Nodes"], v["TotalNodes"] = v["Nodes"][:6], 6
    d.publish(d._parse(c, v, [], d.http[0]))
    assert len(d.missing) == 16
    with pytest.raises(RecoveryUnavailable, match="missing"):
        d.check()


def test_runtime_slot_ids_can_be_reused_between_piles():
    d = ClusterDiscovery("http://localhost:12001")
    c, v = responses(4)
    c["NameserviceConfig"]["Node"] = c["NameserviceConfig"]["Node"][:2]
    d.publish(d._parse(c, v, [], d.http[0]))
    v["Nodes"][2]["NodeId"], v["Nodes"][3]["NodeId"] = 4, 3
    d.publish(d._parse(c, v, [], d.http[0]))
    d.check()
    assert d.node_piles == {1: 0, 2: 1, 4: 0, 3: 1}
    assert len(d._known) == 4 and not d.missing


@pytest.mark.parametrize("same_pile", [False, True])
def test_missing_slot_survives_runtime_id_reuse(same_pile):
    d = ClusterDiscovery("http://localhost:12001")
    c, v = responses(4)
    c["NameserviceConfig"]["Node"] = c["NameserviceConfig"]["Node"][:2]
    if same_pile:
        v["Nodes"][3]["SystemState"]["Location"]["BridgePileName"] = "a"
    d.publish(d._parse(c, v, [], d.http[0]))
    absent = v["Nodes"].pop(2)
    v["TotalNodes"] = 3
    v["Nodes"][2]["NodeId"] = 3
    for _ in range(2):
        snapshot = d._parse(c, v, [], d.http[0])
        d.publish(snapshot)
        assert set(snapshot.inventory.slots) == {10003, 10004}
        with pytest.raises(RecoveryUnavailable, match="missing"):
            d.check()
    absent["NodeId"] = 4
    v["Nodes"].append(absent)
    v["TotalNodes"] = 4
    d.publish(d._parse(c, v, [], d.http[0]))
    d.check()
    assert not d.missing and len(d._known) == 4
