from __future__ import annotations

import random
import socket
import tempfile
import time
from pathlib import Path

import pytest
import ydb
import yaml

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.predicates import blobstorage
from ydb.tests.library.stability.healthcheck.healthcheck_reporter import (
    HealthCheckEndpoint,
    HealthCheckReporter,
)
from ydb.tests.stability.nemesis.internal.nemesis.local_cluster_view import (
    LocalClusterView,
)
from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import use_cluster
from ydb.tests.stability.nemesis.internal.nemesis.catalog import NEMESIS_TYPES
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.boundary_scheduler import (
    BoundaryNemesisScheduler,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_state import (
    ChaosOrchestratorStore,
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
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.metrics import (
    EVENT_BUDGET_ACQUIRED,
    EVENT_BUDGET_RELEASED,
    NemesisMetrics,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.recovery_probe import (
    RecoveryProbe,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.schedule_loop import (
    OrchestratorNemesisSchedule,
)
from ydb.tests.stability.nemesis.routers import agent_router


class _NoopSensor:
    def set(self, value):
        del value

    def inc(self):
        pass


class _NoopMonitor:
    def int_gauge(self, name, labels):
        del name, labels
        return _NoopSensor()

    def counter(self, name, labels):
        del name, labels
        return _NoopSensor()


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


def test_scheduled_kill_restarts_only_one_node_and_releases_its_budget(
    local_multinode_cluster,
    local_multinode_model,
):
    view, _, inventory, guard = local_multinode_model
    endpoints = [
        HealthCheckEndpoint(
            logical_host=node.logical_host,
            connect_host=node.connect_host,
            grpc_port=node.grpc_port,
        )
        for node in view.nodes.values()
    ]
    reporter = HealthCheckReporter(
        endpoints,
        store_results=True,
        publish_results=False,
    )
    # Eight large CLI processes per tick are intentionally paced: a sub-second
    # period starves the workload and measures client-launch pressure, not recovery.
    reporter.hc_period_seconds = 2
    reporter.hc_request_timeout_seconds = 30
    reporter.start_healthchecks()

    expected_hosts = {f"node-{node_id}" for node_id in range(1, 9)}
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if (
            set(reporter.last_results) == expected_hosts
            and all(
                result["self_check_result"] == "GOOD"
                for result in reporter.last_results.values()
            )
        ):
            break
        time.sleep(0.05)
    else:
        pytest.fail("Eight-node healthcheck did not become GOOD before node chaos")

    metrics = NemesisMetrics(
        run_id="local-multinode-kill",
        mon=_NoopMonitor(),
    )
    guard.set_metrics(metrics)
    blind_events = []
    stuck_faults = []
    probe = RecoveryProbe(
        guard=guard,
        hc_source=reporter,
        on_blind=lambda: blind_events.append(time.monotonic()),
        on_stuck=stuck_faults.append,
        poll_interval=0.1,
        min_hold_sec=2,
        max_hc_age_sec=30,
        metrics=metrics,
    )
    probe.start()

    chaos_store = ChaosOrchestratorStore(
        failure_guard=guard,
        inventory=inventory,
        planner_types=["KillNodeNemesis"],
    )
    schedule = OrchestratorNemesisSchedule(
        chaos_store=chaos_store,
        get_hosts=lambda: list(expected_hosts),
        is_local_host=lambda host: host in expected_hosts,
        get_app_port=lambda: 0,
        failure_guard=guard,
        recovery_probe=probe,
        inventory=inventory,
        metrics=metrics,
    )
    scheduler = BoundaryNemesisScheduler(
        guard=guard,
        inventory=inventory,
        plan_inject=chaos_store.plan_inject_target,
        plan_extract=chaos_store.plan_extract_target,
        dispatch=lambda command: schedule.dispatch_command(
            command,
            track_history=True,
        ),
        recovery_probe=probe,
        enabled_types=["KillNodeNemesis"],
        base_interval=5,
        jitter=0,
        max_per_tick=1,
        max_bypass_per_tick=1,
        rng=random.Random(1),
    )

    driver = ydb.Driver(
        ydb.DriverConfig(
            database="/Root",
            endpoint=local_multinode_cluster.nodes[1].endpoint,
        )
    )
    driver.wait(timeout=60)
    process_offset = len(agent_router.manager.get_all())
    pids_before = {
        node_id: node.pid
        for node_id, node in local_multinode_cluster.nodes.items()
    }
    health_update_before = reporter.last_update
    workload_progress = 0
    try:
        with ydb.QuerySessionPool(driver) as pool:
            pool.execute_with_retries(
                """
                CREATE TABLE `local_nemesis_node_restart` (
                    id Uint64 NOT NULL,
                    value Uint64,
                    PRIMARY KEY (id)
                )
                """
            )

            with use_cluster(local_multinode_cluster):
                assert scheduler.status()["enabled_types"] == ["KillNodeNemesis"]
                assert scheduler.status()["max_per_tick"] == 1
                assert scheduler.tick() == 1
                assert guard.snapshot()["tracked_executions"] == 1
                agent_router.wait_for_local_processes()

                executions = agent_router.manager.get_all()[process_offset:]
                assert len(executions) == 1
                assert executions[0]["type"] == "KillNodeNemesis"
                assert executions[0]["status"] == "finished", executions[0]["logs"]
                assert executions[0]["ret_code"] == 0

                deadline = time.monotonic() + 120
                while time.monotonic() < deadline:
                    try:
                        pool.execute_with_retries(
                            f"""
                            UPSERT INTO `local_nemesis_node_restart` (id, value)
                            VALUES (1, {workload_progress})
                            """
                        )
                        result = pool.execute_with_retries(
                            """
                            SELECT value
                            FROM `local_nemesis_node_restart`
                            WHERE id = 1
                            """
                        )
                        assert result[0].rows[0]["value"] == workload_progress
                        workload_progress += 1
                    except ydb.Error:
                        pass
                    if (
                        workload_progress > 0
                        and guard.snapshot()["tracked_executions"] == 0
                    ):
                        break
                    time.sleep(0.05)

        pids_after = {
            node_id: node.pid
            for node_id, node in local_multinode_cluster.nodes.items()
        }
        changed_nodes = [
            node_id
            for node_id in pids_before
            if pids_before[node_id] != pids_after[node_id]
        ]
        assert len(changed_nodes) == 1
        changed_node = changed_nodes[0]
        assert all(
            pids_before[node_id] == pids_after[node_id]
            for node_id in pids_before
            if node_id != changed_node
        )

        assert workload_progress > 0
        assert guard.snapshot()["tracked_executions"] == 0
        assert probe.pending() == []
        assert stuck_faults == []
        assert blind_events == []
        assert reporter.last_update > health_update_before

        storage_recovered = False
        storage_error = None
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            try:
                storage_recovered = (
                    blobstorage.cluster_has_no_unreplicated_vdisks(
                        local_multinode_cluster,
                        max_count=0,
                    )
                    and blobstorage.cluster_has_no_unsynced_vdisks(
                        local_multinode_cluster
                    )
                )
                if storage_recovered:
                    break
            except Exception as error:
                storage_error = error
            time.sleep(0.5)
        assert storage_recovered, storage_error

        snapshot = probe.snapshot_now()
        assert snapshot.fresh is True
        assert snapshot.answering == expected_hosts
        assert snapshot.good == expected_hosts

        events = metrics.recent_events(100)
        acquired = [
            event
            for event in events
            if event["event"] == EVENT_BUDGET_ACQUIRED
        ]
        released = [
            event
            for event in events
            if event["event"] == EVENT_BUDGET_RELEASED
        ]
        assert len(acquired) == 1
        assert len(released) == 1
        assert acquired[0]["lease_id"] == released[0]["lease_id"]
        assert acquired[0]["target"]["node_id"] == changed_node
        assert released[0]["reason"] == "recovered"
        assert all(node.is_alive() for node in local_multinode_cluster.nodes.values())
    finally:
        scheduler.stop()
        probe.stop()
        reporter.stop_healthchecks()
        driver.stop()


def test_stop_start_holds_ports_rotates_log_and_recovers_exact_node(
    local_multinode_cluster,
    local_multinode_model,
):
    view, _, inventory, guard = local_multinode_model
    expected_hosts = {f"node-{node_id}" for node_id in range(1, 9)}
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
    reporter.hc_period_seconds = 2
    reporter.hc_request_timeout_seconds = 30
    reporter.start_healthchecks()

    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if (
            set(reporter.last_results) == expected_hosts
            and all(
                result["self_check_result"] == "GOOD"
                for result in reporter.last_results.values()
            )
        ):
            break
        time.sleep(0.05)
    else:
        pytest.fail("Eight-node healthcheck did not become GOOD before stop/start")

    metrics = NemesisMetrics(
        run_id="local-multinode-stop-start",
        mon=_NoopMonitor(),
    )
    guard.set_metrics(metrics)
    blind_events = []
    stuck_faults = []
    probe = RecoveryProbe(
        guard=guard,
        hc_source=reporter,
        on_blind=lambda: blind_events.append(time.monotonic()),
        on_stuck=stuck_faults.append,
        poll_interval=0.1,
        min_hold_sec=0.5,
        max_hc_age_sec=90,
        metrics=metrics,
    )
    probe.start()

    chaos_store = ChaosOrchestratorStore(
        failure_guard=guard,
        inventory=inventory,
        planner_types=["StopStartNodeNemesis"],
    )
    schedule = OrchestratorNemesisSchedule(
        chaos_store=chaos_store,
        get_hosts=lambda: list(expected_hosts),
        is_local_host=lambda host: host in expected_hosts,
        get_app_port=lambda: 0,
        failure_guard=guard,
        recovery_probe=probe,
        inventory=inventory,
        metrics=metrics,
    )
    scheduler = BoundaryNemesisScheduler(
        guard=guard,
        inventory=inventory,
        plan_inject=chaos_store.plan_inject_target,
        plan_extract=chaos_store.plan_extract_target,
        dispatch=lambda command: schedule.dispatch_command(
            command,
            track_history=True,
        ),
        recovery_probe=probe,
        enabled_types=["StopStartNodeNemesis"],
        recovery_sec_for=lambda nemesis_type: 5,
        base_interval=10,
        jitter=0,
        max_per_tick=1,
        max_bypass_per_tick=1,
        rng=random.Random(2),
    )
    runner = NEMESIS_TYPES["StopStartNodeNemesis"]["runner"]

    driver = ydb.Driver(
        ydb.DriverConfig(
            database="/Root",
            endpoint=local_multinode_cluster.nodes[1].endpoint,
        )
    )
    driver.wait(timeout=60)
    pids_before = {
        node_id: node.pid
        for node_id, node in local_multinode_cluster.nodes.items()
    }
    logs_before = {
        node_id: node.ydbd_log_file_path
        for node_id, node in local_multinode_cluster.nodes.items()
    }
    workload_progress = 0
    saw_non_good = False
    held_since = None
    stopped_node_id = None
    try:
        with ydb.QuerySessionPool(driver) as pool:
            pool.execute_with_retries(
                """
                CREATE TABLE `local_nemesis_stop_start` (
                    id Uint64 NOT NULL,
                    value Uint64,
                    PRIMARY KEY (id)
                )
                """
            )

            with use_cluster(local_multinode_cluster):
                assert scheduler.status()["enabled_types"] == [
                    "StopStartNodeNemesis"
                ]
                assert scheduler.status()["max_per_tick"] == 1
                assert scheduler.tick() == 1
                assert guard.snapshot()["tracked_executions"] == 1
                agent_router.wait_for_local_processes()

                stopped = runner.local_stopped_node_ids()
                assert len(stopped) == 1
                stopped_node_id = stopped[0]
                stopped_node = local_multinode_cluster.nodes[stopped_node_id]
                assert stopped_node.is_alive() is False
                assert all(
                    node.is_alive()
                    for node_id, node in local_multinode_cluster.nodes.items()
                    if node_id != stopped_node_id
                )
                held_ports = runner.local_held_ports(stopped_node_id)
                assert set(held_ports) >= {
                    stopped_node.ic_port,
                    stopped_node.grpc_port,
                    stopped_node.mon_port,
                }
                for port in held_ports:
                    probe_socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                    try:
                        with pytest.raises(OSError):
                            probe_socket.bind(("::", port))
                    finally:
                        probe_socket.close()
                held_since = time.monotonic()

                deadline = time.monotonic() + 120
                while time.monotonic() < deadline:
                    saw_non_good = saw_non_good or any(
                        result["self_check_result"] != "GOOD"
                        for result in reporter.last_results.values()
                    )
                    try:
                        pool.execute_with_retries(
                            f"""
                            UPSERT INTO `local_nemesis_stop_start` (id, value)
                            VALUES (1, {workload_progress})
                            """
                        )
                        result = pool.execute_with_retries(
                            """
                            SELECT value
                            FROM `local_nemesis_stop_start`
                            WHERE id = 1
                            """
                        )
                        assert result[0].rows[0]["value"] == workload_progress
                        workload_progress += 1
                    except ydb.Error:
                        pass
                    if (
                        workload_progress > 0
                        and guard.snapshot()["tracked_executions"] == 0
                        and stopped_node.is_alive()
                    ):
                        break
                    time.sleep(0.05)

                agent_router.wait_for_local_processes()
                pid_after_recovery = stopped_node.pid
                repeated = agent_router.create_process_helper(
                    "StopStartNodeNemesis",
                    "extract",
                    payload={
                        "host": f"node-{stopped_node_id}",
                        "chaos_target": next(
                            target
                            for target in inventory.entities(TargetKind.NODE)
                            if target.node_id == stopped_node_id
                        ).to_dict(),
                    },
                )
                assert repeated["status"] != "error", repeated
                agent_router.wait_for_local_processes()
                assert stopped_node.pid == pid_after_recovery

        assert stopped_node_id is not None
        assert held_since is not None
        assert time.monotonic() - held_since >= 1
        assert saw_non_good is True
        assert workload_progress > 0
        assert runner.local_stopped_node_ids() == ()
        assert guard.snapshot()["tracked_executions"] == 0
        assert probe.pending() == []
        assert stuck_faults == []
        assert blind_events == []

        pids_after = {
            node_id: node.pid
            for node_id, node in local_multinode_cluster.nodes.items()
        }
        assert pids_after[stopped_node_id] != pids_before[stopped_node_id]
        assert all(
            pids_after[node_id] == pids_before[node_id]
            for node_id in pids_before
            if node_id != stopped_node_id
        )
        new_log = local_multinode_cluster.nodes[
            stopped_node_id
        ].ydbd_log_file_path
        assert new_log != logs_before[stopped_node_id]
        assert new_log is not None and Path(new_log).is_file()

        events = metrics.recent_events(100)
        acquired = [
            event
            for event in events
            if event["event"] == EVENT_BUDGET_ACQUIRED
        ]
        released = [
            event
            for event in events
            if event["event"] == EVENT_BUDGET_RELEASED
        ]
        assert len(acquired) == 1
        assert len(released) == 1
        assert acquired[0]["lease_id"] == released[0]["lease_id"]
        assert acquired[0]["target"]["node_id"] == stopped_node_id
        assert released[0]["reason"] == "recovered"
        assert all(node.is_alive() for node in local_multinode_cluster.nodes.values())
    finally:
        with use_cluster(local_multinode_cluster):
            runner.restore_local_harness_nodes()
        scheduler.stop()
        probe.stop()
        reporter.stop_healthchecks()
        driver.stop()
