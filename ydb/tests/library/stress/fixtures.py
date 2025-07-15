# -*- coding: utf-8 -*-
import pytest
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.param_constants import kikimr_driver_path

from ydb.tests.oss.ydb_sdk_import import ydb


class StressFixture:
    @pytest.fixture(autouse=True)
    def base_setup(self):
        self.all_binary_paths = [kikimr_driver_path()]

    def setup_cluster(self, **kwargs):
        self.config = KikimrConfigGenerator(
            binary_paths=self.all_binary_paths,
            **kwargs,
        )

        self.cluster = KiKiMR(self.config)
        self.cluster.start()        
        self.database = "/Root/db"
        self.cluster.create_database(self.database,
                                storage_pool_units_count={
                                    'hdd': 1
                                })
        self.cluster.register_and_start_slots(self.database, 1)
        self.cluster.wait_tenant_up(self.database)

        self.endpoint = "grpc://%s:%s" % ('localhost', self.cluster.nodes[1].port)
        self.mon_endpoint = f"http://localhost:{self.cluster.nodes[1].mon_port}"

        self.driver = ydb.Driver(
            ydb.DriverConfig(
                database=self.database,
                endpoint=self.endpoint
            )
        )
        self.driver.wait()
        yield
        self.cluster.stop()
