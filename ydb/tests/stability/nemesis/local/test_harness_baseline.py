from __future__ import annotations

import time
import tempfile
from pathlib import Path

import pytest
import ydb
import yaml
from flask import Flask

from ydb.tests.library.common.delayed import wait_tablets_state_by_id
from ydb.tests.library.common.types import Erasure, TabletStates, TabletTypes
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.stability.healthcheck.healthcheck_reporter import (
    HealthCheckEndpoint,
    HealthCheckReporter,
)
from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import use_cluster
from ydb.tests.stability.nemesis.internal.nemesis.local_cluster_view import (
    LocalClusterView,
)
from ydb.tests.stability.nemesis.internal.nemesis.runners.cluster_tablets import (
    ClusterKillDataShardNemesis,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.boundary_scheduler import (
    BoundaryNemesisScheduler,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_problems import (
    ChaosProblemStore,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_state import (
    ChaosOrchestratorStore,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.chaos_target import (
    ChaosTarget,
    TargetKind,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.cluster_inventory import (
    ClusterInventory,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import (
    ClusterTopologyModel,
    FailureModelGuard,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.hc_model import (
    build_snapshot,
)
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.schedule_loop import (
    OrchestratorNemesisSchedule,
)
from ydb.tests.stability.nemesis.routers import agent_router


class _SingleTabletInventory:
    nodes = {}
    slots = {}
    hosts = ["localhost"]

    @staticmethod
    def entities(kind):
        if kind is TargetKind.TABLET:
            return [ChaosTarget.for_tablet("localhost")]
        return []

    @staticmethod
    def control_host():
        return "localhost"


def _wait_for_api_execution(client, process_offset, timeout=30):
    deadline = time.monotonic() + timeout
    execution = None
    while time.monotonic() < deadline:
        executions = client.get("/api/processes").get_json()[process_offset:]
        if executions:
            execution = executions[-1]
            if execution["status"] != "running":
                break
        time.sleep(0.1)
    assert execution is not None
    assert execution["status"] == "finished", execution["logs"]
    assert execution["ret_code"] == 0
    return execution


@pytest.fixture(scope="module")
def local_cluster():
    cluster = KiKiMR(KikimrConfigGenerator(erasure=Erasure.NONE, nodes=1))
    cluster.start()
    try:
        yield cluster
    finally:
        cluster.stop()


def test_live_harness_endpoint_serves_a_datashard_table(local_cluster):
    node = local_cluster.nodes[1]
    assert node.host == "localhost"
    assert len({node.grpc_port, node.mon_port, node.ic_port}) == 3

    driver = ydb.Driver(
        ydb.DriverConfig(
            database="/Root",
            endpoint=node.endpoint,
        )
    )
    driver.wait(timeout=60)
    try:
        with ydb.QuerySessionPool(driver) as pool:
            pool.execute_with_retries(
                """
                CREATE TABLE `local_nemesis_baseline` (
                    id Uint64 NOT NULL,
                    value Utf8,
                    PRIMARY KEY (id)
                )
                """
            )
            pool.execute_with_retries(
                """
                UPSERT INTO `local_nemesis_baseline` (id, value)
                VALUES (1, "ready")
                """
            )
            result_sets = pool.execute_with_retries(
                """
                SELECT value
                FROM `local_nemesis_baseline`
                WHERE id = 1
                """
            )
            assert result_sets[0].rows[0]["value"] == "ready"

            tablet_ids = [
                info.TabletId
                for info in local_cluster.client.tablet_state(
                    TabletTypes.FLAT_DATASHARD
                ).TabletStateInfo
            ]
            assert tablet_ids

            nemesis = ClusterKillDataShardNemesis()
            with use_cluster(local_cluster):
                nemesis.inject_fault()

            wait_tablets_state_by_id(
                local_cluster.client,
                TabletStates.Active,
                tablet_ids=tablet_ids,
                timeout_seconds=60,
            )

            recovered = pool.execute_with_retries(
                """
                SELECT value
                FROM `local_nemesis_baseline`
                WHERE id = 1
                """
            )
    finally:
        driver.stop()

    assert recovered[0].rows[0]["value"] == "ready"
    assert nemesis._tablet_ids
    assert nemesis._disabled is False


def test_local_cluster_view_uses_live_ports_and_nameservice_location(local_cluster):
    live_node = local_cluster.nodes[1]
    view = LocalClusterView.from_cluster(local_cluster)
    node = view.nodes[1]

    assert node.logical_host == "node-1"
    assert node.connect_host == live_node.host
    assert (node.grpc_port, node.mon_port, node.ic_port) == (
        live_node.grpc_port,
        live_node.mon_port,
        live_node.ic_port,
    )
    assert node.rack == "1"
    assert view.topology_document()["hosts"] == [
        {
            "name": "node-1",
            "location": {
                "rack": "1",
                "data_center": "1",
            },
        }
    ]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as topology_file:
        yaml.safe_dump(view.topology_document(), topology_file)
        topology_file.flush()
        topology = ClusterTopologyModel(topology_file.name)
        inventory = ClusterInventory(topology, cluster_view=view)

    targets = inventory.entities(TargetKind.NODE)
    assert len(targets) == 1
    assert targets[0].host == "node-1"
    assert targets[0].node_id == 1
    assert targets[0].ic_port == live_node.ic_port


def test_embedded_agent_api_allows_only_datashard_chaos(local_cluster):
    node = local_cluster.nodes[1]
    driver = ydb.Driver(
        ydb.DriverConfig(
            database="/Root",
            endpoint=node.endpoint,
        )
    )
    driver.wait(timeout=60)
    try:
        with ydb.QuerySessionPool(driver) as pool:
            pool.execute_with_retries(
                """
                CREATE TABLE `local_nemesis_manual_api` (
                    id Uint64 NOT NULL,
                    value Utf8,
                    PRIMARY KEY (id)
                )
                """
            )
            pool.execute_with_retries(
                """
                UPSERT INTO `local_nemesis_manual_api` (id, value)
                VALUES (1, "before-chaos")
                """
            )

            tablet_ids = [
                info.TabletId
                for info in local_cluster.client.tablet_state(
                    TabletTypes.FLAT_DATASHARD
                ).TabletStateInfo
            ]
            assert tablet_ids

            app = Flask(__name__)
            app.register_blueprint(agent_router.blueprint)
            with use_cluster(local_cluster), app.test_client() as client:
                process_count = len(agent_router.manager.get_all())
                for unsafe_type in (
                    "NetworkNemesis",
                    "TimeSkewNemesis",
                    "SafelyCleanupDisksNemesis",
                ):
                    response = client.post(
                        "/api/processes",
                        json={"type": unsafe_type, "action": "inject"},
                    )
                    assert response.status_code == 400
                    assert "not enabled for local harness execution" in response.get_json()["message"]
                assert len(agent_router.manager.get_all()) == process_count

                response = client.post(
                    "/api/processes",
                    json={"type": "KillDataShardNemesis", "action": "inject"},
                )
                assert response.status_code == 200
                execution = _wait_for_api_execution(client, process_count)
                assert execution["type"] == "KillDataShardNemesis"

            wait_tablets_state_by_id(
                local_cluster.client,
                TabletStates.Active,
                tablet_ids=tablet_ids,
                timeout_seconds=60,
            )
            recovered = pool.execute_with_retries(
                """
                SELECT value
                FROM `local_nemesis_manual_api`
                WHERE id = 1
                """
            )
    finally:
        driver.stop()

    assert recovered[0].rows[0]["value"] == "before-chaos"


def test_embedded_agent_api_kills_scheme_shard_manually(local_cluster):
    tablet_ids = [
        info.TabletId
        for info in local_cluster.client.tablet_state(
            TabletTypes.FLAT_SCHEMESHARD
        ).TabletStateInfo
    ]
    assert tablet_ids

    app = Flask(__name__)
    app.register_blueprint(agent_router.blueprint)
    with use_cluster(local_cluster), app.test_client() as client:
        process_offset = len(agent_router.manager.get_all())
        response = client.post(
            "/api/processes",
            json={"type": "KillSchemeShardNemesis", "action": "inject"},
        )
        assert response.status_code == 200
        execution = _wait_for_api_execution(client, process_offset)
        assert execution["type"] == "KillSchemeShardNemesis"

    wait_tablets_state_by_id(
        local_cluster.client,
        TabletStates.Active,
        tablet_ids=tablet_ids,
        timeout_seconds=60,
    )

    node = local_cluster.nodes[1]
    driver = ydb.Driver(
        ydb.DriverConfig(
            database="/Root",
            endpoint=node.endpoint,
        )
    )
    driver.wait(timeout=60)
    try:
        with ydb.QuerySessionPool(driver) as pool:
            pool.execute_with_retries(
                """
                CREATE TABLE `local_nemesis_after_scheme_shard` (
                    id Uint64 NOT NULL,
                    PRIMARY KEY (id)
                )
                """
            )
            pool.execute_with_retries(
                """
                UPSERT INTO `local_nemesis_after_scheme_shard` (id)
                VALUES (1)
                """
            )
            result = pool.execute_with_retries(
                """
                SELECT id
                FROM `local_nemesis_after_scheme_shard`
                WHERE id = 1
                """
            )
    finally:
        driver.stop()

    assert result[0].rows[0]["id"] == 1


def test_embedded_agent_api_kills_hive_manually(local_cluster):
    tablet_ids = [
        info.TabletId
        for info in local_cluster.client.tablet_state(
            TabletTypes.FLAT_HIVE
        ).TabletStateInfo
    ]
    assert tablet_ids

    app = Flask(__name__)
    app.register_blueprint(agent_router.blueprint)
    with use_cluster(local_cluster), app.test_client() as client:
        process_offset = len(agent_router.manager.get_all())
        response = client.post(
            "/api/processes",
            json={"type": "KillHiveNemesis", "action": "inject"},
        )
        assert response.status_code == 200
        execution = _wait_for_api_execution(client, process_offset)
        assert execution["type"] == "KillHiveNemesis"

    wait_tablets_state_by_id(
        local_cluster.client,
        TabletStates.Active,
        tablet_ids=tablet_ids,
        timeout_seconds=60,
    )

    node = local_cluster.nodes[1]
    driver = ydb.Driver(
        ydb.DriverConfig(
            database="/Root",
            endpoint=node.endpoint,
        )
    )
    driver.wait(timeout=60)
    try:
        with ydb.QuerySessionPool(driver) as pool:
            pool.execute_with_retries(
                """
                CREATE TABLE `local_nemesis_after_hive` (
                    id Uint64 NOT NULL,
                    PRIMARY KEY (id)
                )
                """
            )
            pool.execute_with_retries(
                """
                UPSERT INTO `local_nemesis_after_hive` (id)
                VALUES (1)
                """
            )
            result = pool.execute_with_retries(
                """
                SELECT id
                FROM `local_nemesis_after_hive`
                WHERE id = 1
                """
            )
    finally:
        driver.stop()

    assert result[0].rows[0]["id"] == 1


def test_healthcheck_uses_the_live_harness_grpc_port(local_cluster):
    node = local_cluster.nodes[1]
    reporter = HealthCheckReporter(
        [
            HealthCheckEndpoint(
                logical_host="node-1",
                connect_host=node.host,
                grpc_port=node.grpc_port,
            )
        ],
        store_results=True,
        publish_results=False,
    )
    reporter.hc_period_seconds = 0.2
    reporter.hc_request_timeout_seconds = 30

    updates = []
    last_update = None
    reporter.start_healthchecks()
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and len(updates) < 2:
            if reporter.last_update is not None and reporter.last_update != last_update:
                result = reporter.last_results.get("node-1")
                if result is not None:
                    updates.append((reporter.last_update, result))
                    last_update = reporter.last_update
            time.sleep(0.05)
    finally:
        reporter.stop_healthchecks()

    assert len(updates) >= 2
    assert all(result["self_check_result"] == "GOOD" for _, result in updates)
    snapshot = build_snapshot(
        reporter.last_results,
        now=time.monotonic(),
        last_update=reporter.last_update,
        max_age_sec=5,
    )
    assert snapshot.fresh is True
    assert snapshot.answering == frozenset({"node-1"})


def test_boundary_scheduler_runs_only_datashard_chaos(local_cluster):
    node = local_cluster.nodes[1]
    driver = ydb.Driver(
        ydb.DriverConfig(
            database="/Root",
            endpoint=node.endpoint,
        )
    )
    driver.wait(timeout=60)
    scheduler = None
    topology_path = None
    try:
        with ydb.QuerySessionPool(driver) as pool:
            pool.execute_with_retries(
                """
                CREATE TABLE `local_nemesis_scheduled` (
                    id Uint64 NOT NULL,
                    value Uint64,
                    PRIMARY KEY (id)
                )
                """
            )

            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".yaml",
                delete=False,
            ) as topology_file:
                yaml.safe_dump(
                    {
                        "static_erasure": "none",
                        "hosts": [
                            {
                                "name": "localhost",
                                "location": {"rack": "local-rack"},
                            }
                        ],
                    },
                    topology_file,
                )
                topology_path = topology_file.name

            topology = ClusterTopologyModel(topology_path)
            inventory = _SingleTabletInventory()
            guard = FailureModelGuard(topology, total_slots=0)
            chaos_store = ChaosOrchestratorStore(
                failure_guard=guard,
                inventory=inventory,
            )
            schedule = OrchestratorNemesisSchedule(
                chaos_store=chaos_store,
                get_hosts=lambda: ["localhost"],
                is_local_host=lambda host: host == "localhost",
                get_app_port=lambda: 0,
                failure_guard=guard,
                inventory=inventory,
            )

            process_offset = len(agent_router.manager.get_all())
            problems = ChaosProblemStore()
            max_running = 0
            workload_progress = 0

            with use_cluster(local_cluster):
                scheduler = BoundaryNemesisScheduler(
                    guard=guard,
                    inventory=inventory,
                    plan_inject=chaos_store.plan_inject_target,
                    dispatch=lambda command: schedule.dispatch_command(
                        command,
                        track_history=True,
                    ),
                    recovery_probe=None,
                    base_interval=1.5,
                    jitter=0,
                    max_per_tick=1,
                    max_bypass_per_tick=1,
                )
                assert scheduler.status()["enabled_types"] == ["KillDataShardNemesis"]
                scheduler.start()

                deadline = time.monotonic() + 30
                finished = []
                while time.monotonic() < deadline:
                    pool.execute_with_retries(
                        f"""
                        UPSERT INTO `local_nemesis_scheduled` (id, value)
                        VALUES (1, {workload_progress})
                        """
                    )
                    result = pool.execute_with_retries(
                        """
                        SELECT value
                        FROM `local_nemesis_scheduled`
                        WHERE id = 1
                        """
                    )
                    assert result[0].rows[0]["value"] == workload_progress
                    workload_progress += 1

                    executions = agent_router.manager.get_all()[process_offset:]
                    running = [item for item in executions if item["status"] == "running"]
                    max_running = max(max_running, len(running))
                    finished = [item for item in executions if item["status"] == "finished"]
                    if len(finished) >= 3:
                        break
                    time.sleep(0.05)

                scheduler.stop()
                agent_router.wait_for_local_processes()

            executions = agent_router.manager.get_all()[process_offset:]
            assert len(finished) >= 3
            assert workload_progress > 3
            assert max_running <= 1
            assert all(item["type"] == "KillDataShardNemesis" for item in executions)
            assert all(item["status"] == "finished" for item in executions)
            assert len(schedule.recent_history(10)) >= 3
            assert scheduler.running() is False
            assert problems.snapshot() == []
    finally:
        if scheduler is not None:
            scheduler.stop()
        if topology_path is not None:
            Path(topology_path).unlink(missing_ok=True)
        driver.stop()
