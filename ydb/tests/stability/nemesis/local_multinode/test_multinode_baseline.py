from __future__ import annotations

import tempfile
import time

import pytest
import yaml

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.stability.healthcheck.healthcheck_reporter import (
    HealthCheckEndpoint,
    HealthCheckReporter,
)
from ydb.tests.stability.nemesis.internal.nemesis.local_cluster_view import (
    LocalClusterView,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import (
    TargetKind,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.cluster_inventory import (
    ClusterInventory,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    ClusterTopologyModel,
    FailureModelGuard,
    ImpactScope,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.recovery_probe import (
    RecoveryProbe,
)


@pytest.fixture(scope="module")
def local_multinode_cluster():
    cluster = KiKiMR(
        KikimrConfigGenerator(
            erasure=Erasure.BLOCK_4_2,
            nodes=8,
            use_in_memory_pdisks=True,
        )
    )
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.stop()


@pytest.fixture(scope="module")
def local_multinode_model(local_multinode_cluster):
    view = LocalClusterView.from_cluster(local_multinode_cluster)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as topology_file:
        yaml.safe_dump(view.topology_document(), topology_file)
        topology_file.flush()
        topology = ClusterTopologyModel(topology_file.name)
        inventory = ClusterInventory(topology, cluster_view=view)
        guard = FailureModelGuard(topology)
        yield view, topology, inventory, guard


def test_inventory_preserves_eight_logical_nodes_and_allocated_ports(
    local_multinode_cluster,
    local_multinode_model,
):
    view, topology, inventory, _ = local_multinode_model

    assert view.static_erasure == "block-4-2"
    assert sorted(node.logical_host for node in view.nodes.values()) == [
        f"node-{node_id}" for node_id in range(1, 9)
    ]
    assert {node.connect_host for node in view.nodes.values()} == {"localhost"}
    assert len({node.grpc_port for node in view.nodes.values()}) == 8
    assert len({node.mon_port for node in view.nodes.values()}) == 8
    assert len({node.ic_port for node in view.nodes.values()}) == 8
    assert len({topology.domain_of(node.logical_host) for node in view.nodes.values()}) == 8

    targets = sorted(
        inventory.entities(TargetKind.NODE),
        key=lambda target: target.node_id,
    )
    assert inventory.degraded_reason is None
    assert len(targets) == 8
    for target in targets:
        live_node = local_multinode_cluster.nodes[target.node_id]
        assert target.host == f"node-{target.node_id}"
        assert target.ic_port == live_node.ic_port


def test_block_4_2_guard_allows_two_domains_and_rejects_a_third(
    local_multinode_model,
):
    _, topology, inventory, guard = local_multinode_model
    targets = sorted(
        inventory.entities(TargetKind.NODE),
        key=lambda target: target.node_id,
    )

    assert topology.tolerance.kind == "domains"
    assert topology.tolerance.max_domains == 2
    leases = []
    for target in targets[:2]:
        lease = guard.reserve(
            guard.footprint_for(target, ImpactScope.NODE),
            target.identity_key(),
        )
        assert lease is not None
        leases.append(lease)

    third = targets[2]
    assert guard.reserve(
        guard.footprint_for(third, ImpactScope.NODE),
        third.identity_key(),
    ) is None
    assert len(guard.snapshot()["impaired_racks"]) == 2

    for lease in leases:
        assert guard.release(lease) is True
    assert guard.snapshot()["tracked_executions"] == 0


def test_all_allocated_endpoints_stay_sighted_for_five_idle_ticks(
    local_multinode_model,
):
    view, _, _, guard = local_multinode_model
    reporter = HealthCheckReporter(
        [
            HealthCheckEndpoint(
                logical_host=node.logical_host,
                connect_host=node.connect_host,
                grpc_port=node.grpc_port,
            )
            for node in view.nodes.values()
        ],
        store_results=True,
        publish_results=False,
    )
    reporter.hc_period_seconds = 0.1
    reporter.hc_request_timeout_seconds = 30

    blind_events = []
    sighted_events = []
    stuck_faults = []
    probe = RecoveryProbe(
        guard=guard,
        hc_source=reporter,
        on_blind=lambda: blind_events.append(time.monotonic()),
        on_sighted=lambda: sighted_events.append(time.monotonic()),
        on_stuck=stuck_faults.append,
        max_hc_age_sec=5,
    )

    expected_hosts = {f"node-{node_id}" for node_id in range(1, 9)}
    ticks = []
    last_update = None
    reporter.start_healthchecks()
    try:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline and len(ticks) < 5:
            if reporter.last_update is not None and reporter.last_update != last_update:
                results = dict(reporter.last_results)
                assert set(results) == expected_hosts
                assert all(
                    result["self_check_result"] == "GOOD"
                    for result in results.values()
                )
                probe.tick()
                ticks.append((reporter.last_update, results))
                last_update = reporter.last_update
            time.sleep(0.05)
    finally:
        reporter.stop_healthchecks()

    assert len(ticks) == 5
    assert blind_events == []
    assert sighted_events == []
    assert stuck_faults == []
    assert probe.snapshot() == {
        "tracked": 0,
        "stuck": 0,
        "confirming": 0,
        "blind": False,
        "faults": [],
    }
