# -*- coding: utf-8 -*-
import pytest
import yatest

from ydb.public.api.grpc import ydb_discovery_v1_pb2_grpc
from ydb.public.api.protos import ydb_discovery_pb2
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture, MixedClusterFixture, RollingUpgradeAndDowngradeFixture

from ydb.tests.oss.ydb_sdk_import import ydb


class SystemTabletBackupTestMixin:
    table_name = "table_name"
    value = 42

    def setup_data(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(f"""
                CREATE TABLE `{self.table_name}` (
                    id Int64 NOT NULL,
                    value Int64 NOT NULL,
                    PRIMARY KEY (id)
                )
            """)
            session_pool.execute_with_retries(
                f"UPSERT INTO `{self.table_name}` (id, value) VALUES (1, {self.value});"
            )

    def check_cluster_aliveness(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            result_sets = session_pool.execute_with_retries(
                f"SELECT id, value FROM `{self.table_name}` WHERE id = 1;"
            )
            assert result_sets[0].rows[0]["id"] == 1, "cluster aliveness check failed"
            assert result_sets[0].rows[0]["value"] == self.value, "cluster aliveness check failed"

    def register_nodes(self, count):
        for i in range(count):
            request = ydb_discovery_pb2.NodeRegistrationRequest(
                host=f"system.tablet.backup.fake.{i}",
                port=19001,
                resolve_host=f"system.tablet.backup.fake.{i}",
                address="594f:10c7:ad54:eada:99eb:7b5b:eec2:0000",
                location=ydb_discovery_pb2.NodeLocation(
                    data_center="DC",
                    module="1",
                    rack="2",
                    unit="3",
                ),
                path="/Root",
            )

            self.driver(
                request,
                ydb_discovery_v1_pb2_grpc.DiscoveryServiceStub,
                "NodeRegistration",
                ydb.operation.Operation,
                None,
                (self.driver,),
            )


class TestSystemTabletBackupMixedCluster(MixedClusterFixture, SystemTabletBackupTestMixin):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (25, 3):
            pytest.skip("System tablet backup works only since 25-3")

        self.backup_path = yatest.common.output_path("system_tablet_backup")
        yield from self.setup_cluster(
            system_tablet_backup_config={
                "filesystem": {
                    "path": self.backup_path,
                },
            },
        )

    def test(self):
        self.setup_data()
        self.register_nodes(10)
        self.check_cluster_aliveness()


class TestSystemTabletBackupRestartToAnotherVersion(RestartToAnotherVersionFixture, SystemTabletBackupTestMixin):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (25, 3):
            pytest.skip("System tablet backup works only since 25-3")

        self.backup_path = yatest.common.output_path("system_tablet_backup")
        yield from self.setup_cluster(
            system_tablet_backup_config={
                "filesystem": {
                    "path": self.backup_path,
                },
            },
        )

    def test(self):
        self.setup_data()
        self.register_nodes(10)
        self.check_cluster_aliveness()

        self.change_cluster_version()

        self.register_nodes(10)
        self.check_cluster_aliveness()


class TestSystemTabletBackupRollingUpdate(RollingUpgradeAndDowngradeFixture, SystemTabletBackupTestMixin):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (25, 3):
            pytest.skip("System tablet backup works only since 25-3")

        self.backup_path = yatest.common.output_path("system_tablet_backup")
        yield from self.setup_cluster(
            system_tablet_backup_config={
                "filesystem": {
                    "path": self.backup_path,
                },
            },
        )

    def test(self):
        self.setup_data()
        self.register_nodes(10)
        self.check_cluster_aliveness()

        for _ in self.roll():
            self.register_nodes(10)
            self.check_cluster_aliveness()
