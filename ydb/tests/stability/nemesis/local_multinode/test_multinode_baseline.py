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
def local_tenant(local_multinode_cluster):
    database = "/Root/local_nemesis_tenant"
    local_multinode_cluster.create_database(database, {"hdd": 1})
    slots = local_multinode_cluster.register_and_start_slots(database, count=2)
    local_multinode_cluster.wait_tenant_up(database)
    try:
        yield database, slots
    finally:
        local_multinode_cluster.unregister_and_stop_slots(slots)
        local_multinode_cluster.remove_database(database)


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


def test_suspend_preserves_pid_stops_progress_and_resumes_exact_node(
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
        pytest.fail("Eight-node healthcheck did not become GOOD before suspend")

    metrics = NemesisMetrics(
        run_id="local-multinode-suspend",
        mon=_NoopMonitor(),
    )
    guard.set_metrics(metrics)
    stuck_faults = []
    probe = RecoveryProbe(
        guard=guard,
        hc_source=reporter,
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
        planner_types=["SuspendNodeNemesis"],
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
        enabled_types=["SuspendNodeNemesis"],
        recovery_sec_for=lambda nemesis_type: 5,
        base_interval=10,
        jitter=0,
        max_per_tick=1,
        max_bypass_per_tick=1,
        rng=random.Random(3),
    )
    runner = NEMESIS_TYPES["SuspendNodeNemesis"]["runner"]

    setup_driver = ydb.Driver(
        ydb.DriverConfig(
            database="/Root",
            endpoint=local_multinode_cluster.nodes[1].endpoint,
        )
    )
    setup_driver.wait(timeout=60)
    pids_before = {
        node_id: node.pid
        for node_id, node in local_multinode_cluster.nodes.items()
    }
    progress_driver = None
    try:
        with ydb.QuerySessionPool(setup_driver) as pool:
            pool.execute_with_retries(
                """
                CREATE TABLE `local_nemesis_suspend` (
                    id Uint64 NOT NULL,
                    value Uint64,
                    PRIMARY KEY (id)
                )
                """
            )

        with use_cluster(local_multinode_cluster):
            assert scheduler.status()["enabled_types"] == ["SuspendNodeNemesis"]
            assert scheduler.status()["max_per_tick"] == 1
            assert scheduler.tick() == 1
            assert guard.snapshot()["tracked_executions"] == 1
            agent_router.wait_for_local_processes()

            frozen = runner.local_frozen_node_ids()
            assert len(frozen) == 1
            frozen_node_id = frozen[0]
            frozen_node = local_multinode_cluster.nodes[frozen_node_id]
            frozen_pid = frozen_node.pid
            assert frozen_pid == pids_before[frozen_node_id]

            deadline = time.monotonic() + 10
            process_state = ""
            while time.monotonic() < deadline:
                status = Path(f"/proc/{frozen_pid}/status").read_text()
                process_state = next(
                    line.split(":", 1)[1].strip()
                    for line in status.splitlines()
                    if line.startswith("State:")
                )
                if process_state.startswith("T"):
                    break
                time.sleep(0.05)
            assert process_state.startswith("T")

            other_node_id = 1 if frozen_node_id != 1 else 2
            progress_driver = ydb.Driver(
                ydb.DriverConfig(
                    database="/Root",
                    endpoint=local_multinode_cluster.nodes[other_node_id].endpoint,
                )
            )
            progress_driver.wait(timeout=60)
            with ydb.QuerySessionPool(progress_driver) as pool:
                pool.execute_with_retries(
                    """
                    UPSERT INTO `local_nemesis_suspend` (id, value)
                    VALUES (1, 1)
                    """
                )
                result = pool.execute_with_retries(
                    """
                    SELECT value
                    FROM `local_nemesis_suspend`
                    WHERE id = 1
                    """
                )
                assert result[0].rows[0]["value"] == 1

            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                if (
                    guard.snapshot()["tracked_executions"] == 0
                    and runner.local_frozen_node_ids() == ()
                ):
                    break
                time.sleep(0.05)

            agent_router.wait_for_local_processes()
            assert frozen_node.pid == frozen_pid
            status = Path(f"/proc/{frozen_pid}/status").read_text()
            process_state = next(
                line.split(":", 1)[1].strip()
                for line in status.splitlines()
                if line.startswith("State:")
            )
            assert not process_state.startswith("T")

            repeated = agent_router.create_process_helper(
                "SuspendNodeNemesis",
                "extract",
                payload={
                    "host": f"node-{frozen_node_id}",
                    "chaos_target": next(
                        target
                        for target in inventory.entities(TargetKind.NODE)
                        if target.node_id == frozen_node_id
                    ).to_dict(),
                },
            )
            assert repeated["status"] != "error", repeated
            agent_router.wait_for_local_processes()
            assert frozen_node.pid == frozen_pid

        assert runner.local_frozen_node_ids() == ()
        assert guard.snapshot()["tracked_executions"] == 0
        assert probe.pending() == []
        assert stuck_faults == []
        assert all(
            node.pid == pids_before[node_id]
            for node_id, node in local_multinode_cluster.nodes.items()
        )

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
        assert acquired[0]["target"]["node_id"] == frozen_node_id
        assert released[0]["reason"] == "recovered"
    finally:
        with use_cluster(local_multinode_cluster):
            runner.restore_local_harness_nodes()
        scheduler.stop()
        probe.stop()
        reporter.stop_healthchecks()
        if progress_driver is not None:
            progress_driver.stop()
        setup_driver.stop()


def test_slot_kill_restarts_one_compute_process_and_releases_slot_budget(
    local_multinode_cluster,
    local_tenant,
):
    database, slots = local_tenant
    view = LocalClusterView.from_cluster(local_multinode_cluster)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as topology_file:
        yaml.safe_dump(view.topology_document(), topology_file)
        topology_file.flush()
        topology = ClusterTopologyModel(topology_file.name)
        inventory = ClusterInventory(topology, cluster_view=view)
        guard = FailureModelGuard(topology, total_slots=len(view.slots))

    targets = sorted(
        inventory.entities(TargetKind.SLOT),
        key=lambda target: target.slot_idx,
    )
    assert len(targets) == 2
    assert len({target.host for target in targets}) == 2
    assert len({target.ic_port for target in targets}) == 2
    assert guard.snapshot()["total_slots"] == 2
    assert guard.snapshot()["max_slots"] == 1

    reporter = HealthCheckReporter(
        [
            HealthCheckEndpoint(
                logical_host=slot.logical_host,
                connect_host=slot.connect_host,
                grpc_port=slot.grpc_port,
                database=database,
            )
            for slot in view.slots.values()
        ],
        store_results=True,
        publish_results=False,
    )
    reporter.hc_period_seconds = 2
    reporter.hc_request_timeout_seconds = 30
    reporter.start_healthchecks()

    metrics = NemesisMetrics(
        run_id="local-multinode-slot-kill",
        mon=_NoopMonitor(),
    )
    guard.set_metrics(metrics)
    stuck_faults = []
    probe = RecoveryProbe(
        guard=guard,
        hc_source=reporter,
        on_stuck=stuck_faults.append,
        poll_interval=0.1,
        min_hold_sec=1,
        max_hc_age_sec=90,
        metrics=metrics,
    )

    deadline = time.monotonic() + 120
    baseline = None
    while time.monotonic() < deadline:
        snapshot = probe.snapshot_now()
        if (
            snapshot.fresh
            and snapshot.good == {slot.logical_host for slot in view.slots.values()}
            and snapshot.alive_compute >= 2
        ):
            baseline = snapshot.alive_compute
            break
        time.sleep(0.05)
    assert baseline is not None and baseline >= 2
    probe.start()

    chaos_store = ChaosOrchestratorStore(
        failure_guard=guard,
        inventory=inventory,
        planner_types=["KillSlotDaemonNemesis"],
    )
    slot_hosts = {target.host for target in targets}
    schedule = OrchestratorNemesisSchedule(
        chaos_store=chaos_store,
        get_hosts=lambda: list(slot_hosts),
        is_local_host=lambda host: host in slot_hosts,
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
        enabled_types=["KillSlotDaemonNemesis"],
        base_interval=5,
        jitter=0,
        max_per_tick=1,
        max_bypass_per_tick=1,
        rng=random.Random(4),
    )

    driver = ydb.Driver(
        ydb.DriverConfig(
            database=database,
            endpoint=slots[0].endpoint,
        )
    )
    driver.wait(timeout=60)
    process_offset = len(agent_router.manager.get_all())
    node_pids_before = {
        node_id: node.pid
        for node_id, node in local_multinode_cluster.nodes.items()
    }
    slot_pids_before = {
        slot_idx: slot.pid
        for slot_idx, slot in local_multinode_cluster.slots.items()
    }
    workload_progress = 0
    try:
        with ydb.QuerySessionPool(driver) as pool:
            pool.execute_with_retries(
                """
                CREATE TABLE `slot_restart` (
                    id Uint64 NOT NULL,
                    value Uint64,
                    PRIMARY KEY (id)
                )
                """
            )
            with use_cluster(local_multinode_cluster):
                assert scheduler.status()["enabled_types"] == [
                    "KillSlotDaemonNemesis"
                ]
                assert scheduler.status()["max_per_tick"] == 1
                assert scheduler.tick() == 1
                assert guard.snapshot()["impaired_slots"] == 1
                agent_router.wait_for_local_processes()

                executions = agent_router.manager.get_all()[process_offset:]
                assert len(executions) == 1
                assert executions[0]["type"] == "KillSlotDaemonNemesis"
                assert executions[0]["status"] == "finished", executions[0]["logs"]
                assert executions[0]["ret_code"] == 0

                deadline = time.monotonic() + 120
                while time.monotonic() < deadline:
                    try:
                        pool.execute_with_retries(
                            f"""
                            UPSERT INTO `slot_restart` (id, value)
                            VALUES (1, {workload_progress})
                            """
                        )
                        result = pool.execute_with_retries(
                            """
                            SELECT value
                            FROM `slot_restart`
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

        slot_pids_after = {
            slot_idx: slot.pid
            for slot_idx, slot in local_multinode_cluster.slots.items()
        }
        changed_slots = [
            slot_idx
            for slot_idx in slot_pids_before
            if slot_pids_before[slot_idx] != slot_pids_after[slot_idx]
        ]
        assert len(changed_slots) == 1
        changed_slot = changed_slots[0]
        assert all(
            slot_pids_before[slot_idx] == slot_pids_after[slot_idx]
            for slot_idx in slot_pids_before
            if slot_idx != changed_slot
        )
        assert all(
            node.pid == node_pids_before[node_id]
            for node_id, node in local_multinode_cluster.nodes.items()
        )
        assert workload_progress > 0
        assert guard.snapshot()["tracked_executions"] == 0
        assert guard.snapshot()["impaired_slots"] == 0
        assert probe.pending() == []
        assert stuck_faults == []
        assert probe.snapshot_now().alive_compute >= baseline

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
        assert acquired[0]["target"]["slot_idx"] == changed_slot
        assert released[0]["reason"] == "recovered"
    finally:
        scheduler.stop()
        probe.stop()
        reporter.stop_healthchecks()
        driver.stop()
