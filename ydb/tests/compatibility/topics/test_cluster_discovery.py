# -*- coding: utf-8 -*-
"""Compatibility tests for Cluster Discovery on an old PQ Cluster schema.

New ydbd creates Balancer if missing and must not ALTER Cluster: CM DestPrepare
CREATE TABLE Cluster matches the historical schema. DiscoverClusters stays
SUCCESS while rolling or restarting onto that binary. FNX names stay hidden
until a Balancer row lists them.
"""
import logging
import time

import grpc
import pytest

from ydb.tests.library.compatibility.fixtures import (
    RestartToAnotherVersionFixture,
    RollingUpgradeAndDowngradeFixture,
    current_binary_path,
    current_name,
)
from ydb.tests.oss.ydb_sdk_import import ydb

try:
    from ydb.public.api.grpc.draft import ydb_persqueue_v1_pb2_grpc
    from ydb.public.api.protos import ydb_persqueue_cluster_discovery_pb2 as cds_pb2
    from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
except ImportError:
    from contrib.ydb.public.api.grpc.draft import ydb_persqueue_v1_pb2_grpc
    from contrib.ydb.public.api.protos import ydb_persqueue_cluster_discovery_pb2 as cds_pb2
    from contrib.ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds


logger = logging.getLogger(__name__)

CLUSTER_TABLE = "/Root/PQ/Config/V2/Cluster"
BALANCER_TABLE = "/Root/PQ/Config/V2/Balancer"
VERSIONS_TABLE = "/Root/PQ/Config/V2/Versions"
DC1 = ("dc1", "dc1.logbroker.yandex.net")
DC2 = ("dc2", "dc2.logbroker.yandex.net")


def _grpc_host(endpoint):
    return endpoint.replace("grpc://", "").replace("grpcs://", "")


def _make_request():
    request = cds_pb2.DiscoverClustersRequest()
    write = request.write_sessions.add()
    write.topic = "compat-topic"
    write.source_id = b"compat-source"
    read = request.read_sessions.add()
    read.all_original.SetInParent()
    return request


def discover_clusters(endpoint, timeout_seconds=120):
    host = _grpc_host(endpoint)
    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        channel = grpc.insecure_channel(host)
        try:
            stub = ydb_persqueue_v1_pb2_grpc.ClusterDiscoveryServiceStub(channel)
            response = stub.DiscoverClusters(_make_request(), timeout=10)
            status = response.operation.status
            if status == StatusIds.SUCCESS:
                result = cds_pb2.DiscoverClustersResult()
                assert response.operation.result.Unpack(result)
                return result
            last_error = "status=%s issues=%s" % (status, response.operation.issues)
            logger.warning("DiscoverClusters not ready: %s", last_error)
        except Exception as exc:
            last_error = repr(exc)
            logger.warning("DiscoverClusters failed: %s", last_error)
        finally:
            channel.close()
        time.sleep(2)
    raise AssertionError("DiscoverClusters did not become SUCCESS: %s" % last_error)


def cluster_names(result):
    names = [cluster.name for cluster in result.read_sessions_clusters[0].clusters]
    names.sort()
    return names


def write_cluster_names(result):
    names = [cluster.name for cluster in result.write_sessions_clusters[0].clusters]
    names.sort()
    return names


def assert_seeded_clusters(result):
    expected = sorted([DC1[0], DC2[0]])
    assert cluster_names(result) == expected
    assert write_cluster_names(result) == expected
    endpoints = {cluster.name: cluster.endpoint for cluster in result.read_sessions_clusters[0].clusters}
    assert endpoints[DC1[0]] == DC1[1]
    assert endpoints[DC2[0]] == DC2[1]
    assert "myt" not in cluster_names(result)


def seed_legacy_cluster_schema(driver):
    for path in ("/Root/PQ", "/Root/PQ/Config", "/Root/PQ/Config/V2"):
        try:
            driver.scheme_client.make_directory(path)
        except Exception as exc:
            logger.info("mkdir %s: %r", path, exc)

    with ydb.QuerySessionPool(driver) as session_pool:
        session_pool.execute_with_retries(
            f"""
            CREATE TABLE IF NOT EXISTS `{CLUSTER_TABLE}` (
                name Utf8,
                balancer Utf8,
                local Bool,
                enabled Bool,
                weight Uint64,
                PRIMARY KEY (name)
            );
            """
        )
        session_pool.execute_with_retries(
            f"""
            CREATE TABLE IF NOT EXISTS `{VERSIONS_TABLE}` (
                name Utf8,
                version Int64,
                PRIMARY KEY (name)
            );
            """
        )
        session_pool.execute_with_retries(
            f"""
            UPSERT INTO `{CLUSTER_TABLE}` (name, balancer, local, enabled, weight) VALUES
                ("{DC1[0]}", "{DC1[1]}", true, true, 1000),
                ("{DC2[0]}", "{DC2[1]}", false, true, 1000);
            UPSERT INTO `{VERSIONS_TABLE}` (name, version) VALUES
                ("Cluster", 1),
                ("Topics", 0);
            """
        )


def schema_has_balancer(driver, timeout_seconds=120):
    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        try:
            with ydb.QuerySessionPool(driver) as session_pool:
                session_pool.execute_with_retries(f"SELECT name, clusters FROM `{BALANCER_TABLE}`;")
            return
        except Exception as exc:
            last_error = repr(exc)
            logger.warning("Waiting for Balancer table: %s", last_error)
            time.sleep(2)
    raise AssertionError("Balancer table was not created: %s" % last_error)


class TestClusterDiscoveryRolling(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(use_legacy_pq=True, enable_pqcd=True)

    def test_discover_clusters_during_roll(self):
        seed_legacy_cluster_schema(self.driver)
        for _ in self.roll():
            result = discover_clusters(self.endpoint)
            assert_seeded_clusters(result)


class TestClusterDiscoveryRestart(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(use_legacy_pq=True, enable_pqcd=True)

    def test_discover_clusters_after_restart(self):
        seed_legacy_cluster_schema(self.driver)
        result = discover_clusters(self.endpoint)
        assert_seeded_clusters(result)

        self.change_cluster_version()
        result = discover_clusters(self.endpoint)
        assert_seeded_clusters(result)

        # Balancer is created only by this change's ydbd.
        # YDB_COMPAT_TARGET_REF defaults to a downloaded prestable, whose
        # name is not "current". Assert the auto-migration when the running
        # binary is the locally built one (YDB_COMPAT_TARGET_REF=current).
        if (
            current_name == "current"
            and self.all_binary_paths[self.current_binary_paths_index] == current_binary_path
        ):
            schema_has_balancer(self.driver)
