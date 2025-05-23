# -*- coding: utf-8 -*-
import copy
import pytest
import yatest
import time
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.param_constants import kikimr_driver_path
from ydb.tests.library.common.types import Erasure
from ydb.tests.oss.ydb_sdk_import import ydb


current_binary_path = kikimr_driver_path()
last_stable_binary_path = yatest.common.binary_path("ydb/tests/library/compatibility/binaries/ydbd-last-stable")
prelast_stable_binary_path = yatest.common.binary_path("ydb/tests/library/compatibility/binaries/ydbd-prelast-stable")

current_name = "current"
last_stable_name = "last"
if last_stable_binary_path is not None:  # in import_test yatest.common.binary_path returns None
    last_stable_name = open(yatest.common.binary_path("ydb/tests/library/compatibility/binaries/ydbd-last-stable-name")).read().strip()
prelast_stable_name = "prelast"
if prelast_stable_binary_path:  # in import_test yatest.common.binary_path returns None
    prelast_stable_name = open(yatest.common.binary_path("ydb/tests/library/compatibility/binaries/ydbd-prelast-stable-name")).read().strip()


all_binary_combinations_restart = [
    [[last_stable_binary_path], [current_binary_path]],
    [[current_binary_path], [last_stable_binary_path]],
    [[current_binary_path], [current_binary_path]],

    [[prelast_stable_binary_path], [last_stable_binary_path]],
    [[last_stable_binary_path], [prelast_stable_binary_path]],
    [[last_stable_binary_path], [last_stable_binary_path]],
]
all_binary_combinations_ids_restart = [
    last_stable_name + "_to_" + current_name,
    current_name + "_to_" + last_stable_name,
    current_name + "_to_" + current_name,

    prelast_stable_name + "_to_" + last_stable_name,
    last_stable_name + "_to_" + prelast_stable_name,
    last_stable_name + "_to_" + last_stable_name,
]


class RestartToAnotherVersionFixture:
    @pytest.fixture(autouse=True, params=all_binary_combinations_restart, ids=all_binary_combinations_ids_restart)
    def base_setup(self, request):
        self.current_binary_paths_index = 0
        self.all_binary_paths = request.param

    def setup_cluster(self, **kwargs):
        extra_feature_flags = kwargs.pop("extra_feature_flags", {})
        extra_feature_flags = copy.copy(extra_feature_flags)
        extra_feature_flags["suppress_compatibility_check"] = True
        self.config = KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            binary_paths=self.all_binary_paths[self.current_binary_paths_index],
            use_in_memory_pdisks=False,
            extra_feature_flags=extra_feature_flags,
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

    def change_cluster_version(self):
        self.current_binary_paths_index = (self.current_binary_paths_index + 1) % len(self.all_binary_paths)
        new_binary_paths = self.all_binary_paths[self.current_binary_paths_index]
        self.config.set_binary_paths(new_binary_paths)
        self.cluster.update_configurator_and_restart(self.config)
        self.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint=self.endpoint
            )
        )
        self.driver.wait()
        # TODO: remove sleep
        # without sleep there are errors like
        # ydb.issues.Unavailable: message: "Failed to resolve tablet: 72075186224037909 after several retries." severity: 1 (server_code: 400050)
        time.sleep(60)


all_binary_combinations_mixed = [
    [current_binary_path],
    [last_stable_binary_path],
    [current_binary_path, last_stable_binary_path],
    [last_stable_binary_path, prelast_stable_binary_path],
]
all_binary_combinations_ids_mixed = [
    current_name,
    last_stable_name,
    current_name + "_and_" + last_stable_name,
    last_stable_name + "_and_" + prelast_stable_name,
]


class MixedClusterFixture:
    @pytest.fixture(autouse=True, params=all_binary_combinations_mixed, ids=all_binary_combinations_ids_mixed)
    def base_setup(self, request):
        self.all_binary_paths = request.param

    def setup_cluster(self, **kwargs):
        self.config = KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            binary_paths=self.all_binary_paths,
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


all_binary_combinations_rolling = [
    [last_stable_binary_path, current_binary_path],
    [prelast_stable_binary_path, last_stable_binary_path],
]
all_binary_combinations_ids_rolling = [
    last_stable_name + "_to_" + current_name,
    prelast_stable_name + "_to_" + last_stable_name,
]


class RollingUpgradeAndDowngradeFixture:
    @pytest.fixture(autouse=True, params=all_binary_combinations_rolling, ids=all_binary_combinations_ids_rolling)
    def base_setup(self, request):
        self.all_binary_paths = request.param

    def setup_cluster(self, **kwargs):
        self.config = KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            binary_paths=[self.all_binary_paths[0]]
            ,
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

    def roll(self):
        # from old to new
        for node_id, node in self.cluster.nodes.items():
            node.stop()
            node.binary_path = self.all_binary_paths[1]
            node.start()
            yield

        # from new to old
        for node_id, node in self.cluster.nodes.items():
            node.stop()
            node.binary_path = self.all_binary_paths[0]

            node.start()
            yield

        yield
