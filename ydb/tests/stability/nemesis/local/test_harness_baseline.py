from __future__ import annotations

import time

import pytest
import ydb
from flask import Flask

from ydb.tests.library.common.delayed import wait_tablets_state_by_id
from ydb.tests.library.common.types import Erasure, TabletStates, TabletTypes
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import use_cluster
from ydb.tests.stability.nemesis.internal.nemesis.runners.cluster_tablets import (
    ClusterKillDataShardNemesis,
)
from ydb.tests.stability.nemesis.routers import agent_router


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

                deadline = time.monotonic() + 30
                execution = None
                while time.monotonic() < deadline:
                    executions = client.get("/api/processes").get_json()
                    execution = executions[-1]
                    if execution["status"] != "running":
                        break
                    time.sleep(0.1)

                assert execution is not None
                assert execution["type"] == "KillDataShardNemesis"
                assert execution["status"] == "finished", execution["logs"]
                assert execution["ret_code"] == 0

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
