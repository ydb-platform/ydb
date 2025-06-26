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
            hive_config={
                "min_scatter_to_balance": 0.01,
                "min_cpuscatter_to_balance": 0.01,
                "min_node_usage_to_balance": 0.01,
                "tablet_kick_cooldown_period": 5,
            },
            **kwargs,
        )

        self.cluster = KiKiMR(self.config)
        self.cluster.start()
        self.endpoint = "grpc://%s:%s" % ('localhost', self.cluster.nodes[1].port)

        self.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint=self.endpoint
            )
        )
        self.driver.wait()
        yield
        self.cluster.stop()
