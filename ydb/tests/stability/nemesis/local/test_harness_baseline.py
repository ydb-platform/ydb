from __future__ import annotations

import pytest
import ydb

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR


@pytest.fixture
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
    finally:
        driver.stop()

    assert result_sets[0].rows[0]["value"] == "ready"
