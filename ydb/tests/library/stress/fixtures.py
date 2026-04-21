# -*- coding: utf-8 -*-
import pytest
import yatest.common
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import Erasure, KikimrConfigGenerator
from ydb.tests.library.harness.param_constants import kikimr_driver_path

from ydb.tests.oss.ydb_sdk_import import ydb


class StressFixture:
    @pytest.fixture(autouse=True)
    def base_setup(self):
        self.all_binary_paths = [kikimr_driver_path()]
        self.base_erasure = yatest.common.get_param('stress_default_erasure', default='NONE')
        self.base_erasure = Erasure.from_string(self.base_erasure)

        self.base_duration = yatest.common.get_param('stress_default_duration', default='60')

    def setup_cluster(self, erasure=None, create_serverless_databases=False, **kwargs):
        erasure = erasure or self.base_erasure
        self.config = KikimrConfigGenerator(
            binary_paths=self.all_binary_paths,
            erasure=erasure,
            **kwargs,
        )

        self.cluster = KiKiMR(self.config)
        self.cluster.start()
        self.database = "/Root"
        self.endpoint = "grpc://%s:%s" % ('localhost', self.cluster.nodes[1].port)
        self.mon_endpoint = f"http://localhost:{self.cluster.nodes[1].mon_port}"
        self.http_proxy_endpoint = f"http://localhost:{self.cluster.nodes[1].http_proxy_port}"

        if create_serverless_databases:
            self._create_serverless_databases()

        self.driver = ydb.Driver(
            ydb.DriverConfig(
                database=self.database,
                endpoint=self.endpoint
            )
        )

        self.driver.wait(timeout=60)
        yield
        self.driver.stop()
        self.cluster.stop()

    def get_kafka_api_ports(self):
        ports = []
        for node in self.cluster.nodes.values():
            ports.append(node.get_kafka_api_port())
        return ports

    def _create_serverless_databases(self):
        timeout_seconds = 30
        self.shared_database_name = "/Root/shared_db"

        self.cluster.remove_database(
            self.shared_database_name,
            timeout_seconds=timeout_seconds
        )

        self.cluster.create_hostel_database(
            self.shared_database_name,
            storage_pool_units_count={
                'hdd': 1
            },
            timeout_seconds=timeout_seconds
        )

        database_nodes = self.cluster.register_and_start_slots(self.shared_database_name, count=3, encryption_key=None)
        self.cluster.wait_tenant_up(self.shared_database_name)

        self.serverless_database_name = "/Root/serverless_db"

        self.cluster.remove_database(
        self.serverless_database_name,
        timeout_seconds=timeout_seconds
        )

        self.cluster.create_serverless_database(
            self.serverless_database_name,
            hostel_db=self.shared_database_name,
            timeout_seconds=timeout_seconds,
            schema_quotas=None, # нужны ли?
            disk_quotas=None, # нужны ли?
            attributes={
                "cloud_id": "CLOUD_ID_VAL",
                "folder_id": "FOLDER_ID_VAL",
                "database_id": "DATABASE_ID_VAL",
            },
        )
