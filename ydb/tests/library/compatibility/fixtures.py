# -*- coding: utf-8 -*-
import copy
import os
import pytest
import time
import yatest
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from ydb.tests.oss.ydb_sdk_import import ydb


def string_version_to_tuple(s):
    result = []
    s = s.replace('.', '-')
    version_components = s.split("-")
    for idx, elem in enumerate(version_components):
        if idx == 0:
            # skipping 'stable' in stable-25-1-1 version
            if elem == 'stable':
                continue
            elif elem == 'current':
                result.append(float('+inf'))
                continue
        elif idx == len(version_components) - 1:
            # skipping 'hotfix' in stable-24-4-4-hotfix version
            if elem == 'hotfix':
                continue
        try:
            result.append(int(elem))
        except ValueError:
            result.append(float('NaN'))
    return tuple(result)


current_binary_path = os.environ.get('YDB_CURRENT_BINARY_PATH', yatest.common.binary_path("ydb/tests/library/compatibility/binaries/ydbd-target"))
current_name = 'current'
if current_binary_path is not None:
    with open(yatest.common.binary_path("ydb/tests/library/compatibility/binaries/ydbd-target-name")) as f:
        current_name = f.read().strip()
current_binary_version = string_version_to_tuple(current_name)

inter_stable_binary_path = os.environ.get('YDB_INTER_BINARY_PATH', yatest.common.binary_path("ydb/tests/library/compatibility/binaries/ydbd-inter"))
init_stable_binary_path = os.environ.get('YDB_INIT_BINARY_PATH', yatest.common.binary_path("ydb/tests/library/compatibility/binaries/ydbd-init"))

inter_stable_version = None
init_stable_version = None

inter_stable_name = 'intermediate'
if inter_stable_binary_path is not None:  # in import_test yatest.common.binary_path returns None
    with open(yatest.common.binary_path("ydb/tests/library/compatibility/binaries/ydbd-inter-name")) as f:
        inter_stable_name = f.read().strip()
        inter_stable_version = string_version_to_tuple(inter_stable_name)
init_stable_name = 'initial'
if init_stable_binary_path:  # in import_test yatest.common.binary_path returns None
    with open(yatest.common.binary_path("ydb/tests/library/compatibility/binaries/ydbd-init-name")) as f:
        init_stable_name = f.read().strip()
        init_stable_version = string_version_to_tuple(init_stable_name)

path_to_version = {
    current_binary_path: current_binary_version,
    inter_stable_binary_path: inter_stable_version,
    init_stable_binary_path: init_stable_version,
}

all_binary_combinations_restart = [
    [inter_stable_binary_path, current_binary_path],
    [current_binary_path, inter_stable_binary_path],
    [current_binary_path, current_binary_path],

    [init_stable_binary_path, inter_stable_binary_path],
    [inter_stable_binary_path, init_stable_binary_path],
    [inter_stable_binary_path, inter_stable_binary_path],
]
all_binary_combinations_ids_restart = [
    "restart_{}_to_{}".format(inter_stable_name, current_name),
    "restart_{}_to_{}".format(current_name, inter_stable_name),
    "restart_{}_to_{}".format(current_name, current_name),

    "restart_{}_to_{}".format(init_stable_name, inter_stable_name),
    "restart_{}_to_{}".format(inter_stable_name, init_stable_name),
    "restart_{}_to_{}".format(inter_stable_name, inter_stable_name),
]


class RestartToAnotherVersionFixture:
    @pytest.fixture(autouse=True, params=all_binary_combinations_restart, ids=all_binary_combinations_ids_restart)
    def base_setup(self, request):
        self.current_binary_paths_index = 0
        self.all_binary_paths = request.param
        self.versions = [path_to_version[path] for path in self.all_binary_paths]

    def setup_cluster(self, **kwargs):
        extra_feature_flags = kwargs.pop("extra_feature_flags", {})
        extra_feature_flags = copy.copy(extra_feature_flags)
        extra_feature_flags["suppress_compatibility_check"] = True
        self.config = KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            binary_paths=[self.all_binary_paths[self.current_binary_paths_index]],
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
        self.driver.wait(timeout=60)
        yield
        self.cluster.stop()

    def change_cluster_version(self):
        self.current_binary_paths_index = (self.current_binary_paths_index + 1) % len(self.all_binary_paths)
        new_binary_paths = self.all_binary_paths[self.current_binary_paths_index]
        self.config.set_binary_paths([new_binary_paths])
        self.cluster.update_configurator_and_restart(self.config)
        self.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint=self.endpoint
            )
        )
        self.driver.wait(timeout=60)
        # TODO: remove sleep
        # without sleep there are errors like
        # ydb.issues.Unavailable: message: "Failed to resolve tablet: 72075186224037909 after several retries." severity: 1 (server_code: 400050)
        time.sleep(60)


all_binary_combinations_mixed = [
    [current_binary_path],
    [inter_stable_binary_path],
    [current_binary_path, inter_stable_binary_path],
    [inter_stable_binary_path, init_stable_binary_path],
]
all_binary_combinations_ids_mixed = [
    "mixed_{}".format(current_name),
    "mixed_{}".format(inter_stable_name),
    "mixed_{}".format(current_name + "_and_" + inter_stable_name),
    "mixed_{}".format(inter_stable_name + "_and_" + init_stable_name),
]


class MixedClusterFixture:
    @pytest.fixture(autouse=True, params=all_binary_combinations_mixed, ids=all_binary_combinations_ids_mixed)
    def base_setup(self, request):
        self.all_binary_paths = request.param
        self.versions = list([path_to_version[path] for path in self.all_binary_paths])

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
        self.driver.wait(timeout=60)
        yield
        self.cluster.stop()


all_binary_combinations_rolling = [
    [inter_stable_binary_path, current_binary_path],
    [init_stable_binary_path, inter_stable_binary_path],
]
all_binary_combinations_ids_rolling = [
    "rolling_{}_to_{}".format(inter_stable_name, current_name),
    "rolling_{}_to_{}".format(init_stable_name, inter_stable_name),
]


class RollingUpgradeAndDowngradeFixture:
    recreate_driver = True  # TODO: temporary workaround. We don't want to recreate driver, but not working now

    @pytest.fixture(autouse=True, params=all_binary_combinations_rolling, ids=all_binary_combinations_ids_rolling)
    def base_setup(self, request):
        self.all_binary_paths = request.param
        self.versions = list([path_to_version[path] for path in self.all_binary_paths])

    def _wait_for_readiness(self):
        if self.recreate_driver:
            self.driver = ydb.Driver(
                ydb.DriverConfig(
                    database='/Root',
                    endpoint=self.endpoints[0]
                )
            )
            self.driver.wait(timeout=60)

        query = """
            CREATE TABLE `test_readiness` (
            id Int64 NOT NULL,
            PRIMARY KEY (id)
        ) """
        timeout = 120  # seconds
        interval = 2  # seconds

        start_time = time.time()
        last_exception = None
        while time.time() - start_time < timeout:
            try:
                with ydb.QuerySessionPool(self.driver) as session_pool:
                    session_pool.execute_with_retries(query, retry_settings=ydb.RetrySettings(max_retries=1))
                break
            except Exception as e:
                last_exception = e
                time.sleep(interval)
        else:
            raise last_exception
        query = """DROP TABLE `test_readiness`"""
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(query)

    def setup_cluster(self, **kwargs):
        extra_feature_flags = kwargs.pop("extra_feature_flags", {})
        extra_feature_flags = copy.copy(extra_feature_flags)
        extra_feature_flags["suppress_compatibility_check"] = True
        # We want to drain tablets before stopping, to prevent "Failed to resolve tablet: 72075186224037909 after several retries"
        # By default draining is not enabled to faster tests
        extra_feature_flags["enable_drain_on_shutdown"] = True
        self.config = KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            binary_paths=[self.all_binary_paths[0]],
            use_in_memory_pdisks=False,
            extra_feature_flags=extra_feature_flags,
            **kwargs,
        )

        self.cluster = KiKiMR(self.config)
        self.cluster.start()
        self.endpoints = []
        for i in range(1, len(self.cluster.nodes) + 1):
            self.endpoints.append("grpc://%s:%s" % ('localhost', self.cluster.nodes[i].port))

        self.endpoint = self.endpoints[0]

        self.driver = ydb.Driver(
            ydb.DriverConfig(
                self.endpoints[0],
                database='/Root'
            )
        )
        self.driver.wait(timeout=60)
        yield
        self.cluster.stop()

    def roll(self):
        # from old to new
        yield
        for node_id, node in self.cluster.nodes.items():
            node.stop()
            node.binary_path = self.all_binary_paths[1]
            node.start()
            self._wait_for_readiness()
            yield

        # from new to old
        for node_id, node in self.cluster.nodes.items():
            node.stop()
            node.binary_path = self.all_binary_paths[0]
            node.start()
            self._wait_for_readiness()
            yield
