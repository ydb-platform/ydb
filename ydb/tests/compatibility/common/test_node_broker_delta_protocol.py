# -*- coding: utf-8 -*-
import os
import pytest
import textwrap
import yatest
from ydb.public.api.grpc import ydb_discovery_v1_pb2_grpc
from ydb.public.api.protos import ydb_discovery_pb2
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture, MixedClusterFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class NodeBrokerTestMixin:
    def setup_class(self):
        self.next_port = 0
        self.table_name = "table_name"
        self.value = 42

    def setup_dynamic_config(self):
        config_path = os.path.join(yatest.common.test_output_path(), "config.yaml")
        with open(config_path, "w") as config:
            config.write(textwrap.dedent("""
                metadata:
                    kind: MainConfig
                    cluster: ""
                    version: 0
                config:
                    yaml_config_enabled: true
                    feature_flags:
                        enable_node_broker_delta_protocol: true
            """))
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint",
            "grpc://localhost:%d" % self.cluster.nodes[1].port,
            "--database=/Root",
            "admin",
            "config",
            "replace",
            "-f",
            config_path,
            "--allow-unknown-fields"
        ]
        yatest.common.execute(cmd, wait=True)

    def setup_data(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                    CREATE TABLE `{self.table_name}` (
                    id Int64 NOT NULL,
                    value Int64 NOT NULL,
                    PRIMARY KEY (id)
                ) """
            session_pool.execute_with_retries(query)

            query = f"""UPSERT INTO `{self.table_name}` (id, value) VALUES (1, {self.value});"""
            session_pool.execute_with_retries(query)

    def register_node(self):
        request = ydb_discovery_pb2.NodeRegistrationRequest(
            host="localhost",
            port=self.next_port,
            resolve_host="localhost",
            address="594f:10c7:ad54:eada:99eb:7b5b:eec2:4490",
            location=ydb_discovery_pb2.NodeLocation(
                data_center="DC",
                module="1",
                rack="2",
                unit="3",
            ),
            path="/Root",
        )

        self.next_port += 1

        self.driver(
            request,
            ydb_discovery_v1_pb2_grpc.DiscoveryServiceStub,
            "NodeRegistration",
            ydb.operation.Operation,
            None,
            (self.driver,),
        )

    def check_cluster_aliveness(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""SELECT id, value FROM `{self.table_name}` WHERE id = 1;"""
            result_sets = session_pool.execute_with_retries(query)

            assert result_sets[0].rows[0]["id"] == 1, "cluster aliveness check failed"
            assert result_sets[0].rows[0]["value"] == self.value, "cluster aliveness check failed"


class TestNodeBrokerDeltaProtocolMixedCluster(MixedClusterFixture, NodeBrokerTestMixin):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (25, 1):
            pytest.skip("Dynamic config in tests applies only since 25-1")

        yield from self.setup_cluster()

    def test(self):
        self.setup_dynamic_config()
        self.setup_data()

        # Register new nodes
        for _ in range(10):
            self.register_node()

        self.check_cluster_aliveness()


class TestNodeBrokerDeltaProtocolRestartToAnotherVersion(RestartToAnotherVersionFixture, NodeBrokerTestMixin):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (25, 1):
            pytest.skip("Dynamic config in tests applies only since 25-1")

        yield from self.setup_cluster()

    def test(self):
        self.setup_dynamic_config()
        self.setup_data()

        # Register new nodes
        for _ in range(10):
            self.register_node()

        self.check_cluster_aliveness()

        self.change_cluster_version()

        # Register new nodes
        for _ in range(10):
            self.register_node()

        self.check_cluster_aliveness()


class TestNodeBrokerDeltaProtocolRollingUpdate(RollingUpgradeAndDowngradeFixture, NodeBrokerTestMixin):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (25, 1):
            pytest.skip("Dynamic config in tests applies only since 25-1")

        yield from self.setup_cluster()

    def test(self):
        self.setup_dynamic_config()
        self.setup_data()

        # Register new nodes
        for _ in range(10):
            self.register_node()

        self.check_cluster_aliveness()

        for _ in self.roll():  # every iteration is a step in rolling upgrade process
            self.register_node()
            self.check_cluster_aliveness()
