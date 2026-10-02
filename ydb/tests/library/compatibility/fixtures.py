# -*- coding: utf-8 -*-
import copy
import logging
import os
import pytest
import time
import yatest
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.fixtures import ydb_database_ctx
from ydb.tests.library.common.types import Erasure
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


def string_version_to_tuple(s):
    result = []
    s = s.replace('.', '-')
    version_components = s.split("-")
    for idx, elem in enumerate(version_components):
        if idx == 0:
            # skipping 'stable' in stable-25-1-1 version
            if elem in ['stable', 'prestable']:
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


def prepare_feature_flags(extra_feature_flags, disabled_feature_flags):
    disabled_feature_flags = copy.copy(disabled_feature_flags)
    assert isinstance(disabled_feature_flags, list), "Feature flags must be list"
    disabled_feature_flags.append("enable_graceful_shutdown")

    extra_feature_flags = copy.copy(extra_feature_flags)
    assert isinstance(extra_feature_flags, list), "Feature flags must be list"
    extra_feature_flags.append("suppress_compatibility_check")

    if "enable_drain_on_shutdown" not in disabled_feature_flags:
        # We want to drain tablets before stopping, to prevent "Failed to resolve tablet: 72075186224037909 after several retries"
        # By default draining is not enabled for faster tests
        extra_feature_flags.append("enable_drain_on_shutdown")

    return extra_feature_flags, disabled_feature_flags


def prepare_table_service_config(table_service_config):
    table_service_config = copy.copy(table_service_config or {})

    if "enable_compile_cache_warmup" not in table_service_config:
        table_service_config["enable_compile_cache_warmup"] = False

    return table_service_config


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

    def stop_driver(self):
        if self.driver is not None:
            self.driver.stop()
            self.driver = None

    def create_driver(self):
        driver = ydb.Driver(
            ydb.DriverConfig(
                database=self.database_path,
                endpoint=self.endpoint,
            )
        )
        driver.wait(timeout=60)
        return driver

    def setup_cluster(self, tenant_db=None, **kwargs):
        extra_feature_flags, disabled_feature_flags = prepare_feature_flags(kwargs.pop("extra_feature_flags", []), kwargs.pop("disabled_feature_flags", []))
        self.config = KikimrConfigGenerator(
            erasure=kwargs.pop("erasure", Erasure.MIRROR_3_DC),
            binary_paths=[self.all_binary_paths[self.current_binary_paths_index]],
            use_in_memory_pdisks=kwargs.pop("use_in_memory_pdisks", False),
            extra_feature_flags=extra_feature_flags,
            disabled_feature_flags=disabled_feature_flags,
            table_service_config=prepare_table_service_config(kwargs.pop("table_service_config", {})),
            **kwargs,
        )

        self.cluster = KiKiMR(self.config)
        self.cluster.start()
        self.endpoint = "grpc://%s:%s" % ('localhost', self.cluster.nodes[1].port)
        self.http_proxy_endpoint = "http://%s:%s" % ('localhost', self.cluster.nodes[1].http_proxy_port)

        if tenant_db is not None:
            with ydb_database_ctx(self.cluster, f"/Root/{tenant_db}", node_count=3) as db_path:
                self.database_path = db_path
                self.driver = self.create_driver()
                yield
        else:
            self.database_path = "/Root"
            self.driver = self.create_driver()
            yield

        self.stop_driver()
        self.cluster.stop()

    def change_cluster_version(self):
        self.current_binary_paths_index = (self.current_binary_paths_index + 1) % len(self.all_binary_paths)
        new_binary_paths = self.all_binary_paths[self.current_binary_paths_index]
        self.config.set_binary_paths([new_binary_paths])

        self.stop_driver()
        self.cluster.update_configurator_and_restart(self.config)
        self.driver = self.create_driver()

        # TODO: remove sleep
        # without sleep there are errors like
        # ydb.issues.Unavailable: message: "Failed to resolve tablet: 72075186224037909 after several retries." severity: 1 (server_code: 400050)
        logger.info("Waiting for cluster initialization")
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

    def stop_driver(self):
        if self.driver is not None:
            self.driver.stop()
            self.driver = None

    def create_driver(self):
        driver = ydb.Driver(
            ydb.DriverConfig(
                database=self.database_path,
                endpoint=self.endpoint
            )
        )
        driver.wait(timeout=60)
        return driver

    def setup_cluster(self, tenant_db=None, **kwargs):
        extra_feature_flags, disabled_feature_flags = prepare_feature_flags(kwargs.pop("extra_feature_flags", []), kwargs.pop("disabled_feature_flags", []))
        all_versions_numbered = all(
            # +inf == current will be float, all other versions are int
            isinstance(item, int)
            for tpl in self.versions
            for item in tpl
        )
        self.config = KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            binary_paths=self.all_binary_paths,
            suppress_version_check=not all_versions_numbered,
            extra_feature_flags=extra_feature_flags,
            disabled_feature_flags=disabled_feature_flags,
            table_service_config=prepare_table_service_config(kwargs.pop("table_service_config", {})),
            **kwargs,
        )

        self.cluster = KiKiMR(self.config)
        self.cluster.start()
        self.endpoint = "grpc://%s:%s" % ('localhost', self.cluster.nodes[1].port)
        self.http_proxy_endpoint = "http://%s:%s" % ('localhost', self.cluster.nodes[1].http_proxy_port)

        if tenant_db is not None:
            with ydb_database_ctx(self.cluster, f"/Root/{tenant_db}", node_count=3) as db_path:
                self.database_path = db_path
                self.driver = self.create_driver()
                yield
        else:
            self.database_path = "/Root"
            self.driver = self.create_driver()
            yield

        self.stop_driver()
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

    def stop_driver(self):
        if self.driver is not None:
            self.driver.stop()
            self.driver = None

    def create_driver(self):
        driver = ydb.Driver(
            ydb.DriverConfig(
                database=self.database_path,
                endpoint=self.endpoints[0]
            )
        )
        driver.wait(timeout=60)
        return driver

    def _new_readiness_table_name(self):
        self._readiness_seq = getattr(self, "_readiness_seq", 0) + 1
        return "test_readiness_%d" % self._readiness_seq

    def _execute_scheme_query(self, query, settings):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                query,
                retry_settings=ydb.RetrySettings(max_retries=1),
                settings=settings,
            )

    def _drop_readiness_table(self, table_name, settings):
        # The probe CREATE can outlive the client timeout. Retry the DROP so the
        # next roll() step does not collide with a path still in EPathStateCreate.
        query = "DROP TABLE IF EXISTS `%s`" % table_name
        deadline = time.time() + 30
        while True:
            try:
                self._execute_scheme_query(query, settings)
                return
            except Exception as e:
                if time.time() >= deadline:
                    logger.warning("Failed to drop readiness table %s: %r", table_name, e)
                    return
                logger.warning("Drop readiness table %s failed, retrying: %r", table_name, e)
                time.sleep(2)

    def _wait_for_readiness(self):
        if self.recreate_driver:
            self.driver = self.create_driver()

        timeout = 120  # seconds
        interval = 2  # seconds
        # Scheme ops during a rolling restart often outlive a 10s cancel window
        # and stay in EPathStateCreate after the client has given up.
        request_timeout = 30  # seconds
        settings = (
            ydb.BaseRequestSettings()
            .with_timeout(request_timeout)
            .with_operation_timeout(request_timeout)
            .with_cancel_after(request_timeout)
        )

        start_time = time.time()
        deadline = start_time + timeout
        last_exception = None
        attempt = 0
        # One fixed name across retries collides with the previous iteration's
        # still-running CREATE/DROP (Overloaded: path exists but creating right now).
        table_name = self._new_readiness_table_name()
        while time.time() < deadline:
            attempt += 1
            query = """
            CREATE TABLE `%s` (
                id Int64 NOT NULL,
                PRIMARY KEY (id)
            ) """ % table_name
            try:
                logger.info("Readiness check attempt %d on %s", attempt, table_name)
                self._execute_scheme_query(query, settings)
                self._drop_readiness_table(table_name, settings)
                return
            except Exception as e:
                message = str(e)
                if "path exist" in message and "creating right now" not in message:
                    logger.info("Readiness table %s already exists", table_name)
                    self._drop_readiness_table(table_name, settings)
                    return
                last_exception = e
                logger.warning(
                    "Readiness check attempt %d failed after %.1fs: %r",
                    attempt,
                    time.time() - start_time,
                    e,
                )
                if "creating right now" in message:
                    table_name = self._new_readiness_table_name()
                time.sleep(interval)

        raise last_exception or RuntimeError("readiness check timed out")

    def setup_cluster(self, tenant_db=None, **kwargs):
        extra_feature_flags, disabled_feature_flags = prepare_feature_flags(kwargs.pop("extra_feature_flags", []), kwargs.pop("disabled_feature_flags", []))
        self.config = KikimrConfigGenerator(
            erasure=kwargs.pop("erasure", Erasure.MIRROR_3_DC),
            binary_paths=[self.all_binary_paths[0]],
            use_in_memory_pdisks=kwargs.pop("use_in_memory_pdisks", False),
            extra_feature_flags=extra_feature_flags,
            disabled_feature_flags=disabled_feature_flags,
            table_service_config=prepare_table_service_config(kwargs.pop("table_service_config", {})),
            **kwargs,
        )

        self.cluster = KiKiMR(self.config)
        self.cluster.start()
        self.endpoints = []
        self.http_proxy_endpoints = []
        for i in range(1, len(self.cluster.nodes) + 1):
            self.endpoints.append("grpc://%s:%s" % ('localhost', self.cluster.nodes[i].port))
            self.http_proxy_endpoints.append("http://%s:%s" % ('localhost', self.cluster.nodes[i].http_proxy_port))

        self.endpoint = self.endpoints[0]
        self.http_proxy_endpoint = self.http_proxy_endpoints[0]

        if tenant_db is not None:
            with ydb_database_ctx(self.cluster, f"/Root/{tenant_db}", node_count=3) as db_path:
                self.database_path = db_path
                self.driver = self.create_driver()
                yield
        else:
            self.database_path = "/Root"
            self.driver = self.create_driver()
            yield

        self.stop_driver()
        self.cluster.stop()

    def roll(self):
        all_nodes = [(id, n, "node") for id, n in self.cluster.nodes.items()] + \
            [(id, n, "slot") for id, n in self.cluster.slots.items()]

        # from old to new
        yield
        for node_id, node, role in all_nodes:
            logger.info(f"upgrading {role} {node_id}")

            if self.recreate_driver:
                # All gRPC channels to the ydbd must be stopped before the node is stopped.
                # Otherwise, the graceful shutdown time for the gRPC server in ydbd is 20-30 seconds,
                # since the channels on the python client side do not close quickly in this case.
                self.stop_driver()
            node.stop()

            node.binary_path = self.all_binary_paths[1]
            node.set_log_file_prefix("logfile_upgraded_")
            node.start()
            self._wait_for_readiness()
            yield

        # from new to old
        for node_id, node, role in all_nodes:
            logger.info(f"downgrading {role} {node_id}")

            if self.recreate_driver:
                self.stop_driver()
            node.stop()

            node.binary_path = self.all_binary_paths[0]
            node.set_log_file_prefix("logfile_downgraded_")
            node.start()
            self._wait_for_readiness()
            yield


# Starts with a new cluster and downgrades it. Useful for testing new features that may be absent from the previous release.
class RollingDowngradeAndUpgradeFixture(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, params=all_binary_combinations_rolling, ids=all_binary_combinations_ids_rolling)
    def base_setup(self, request):
        self.all_binary_paths = request.param[::-1]
        self.versions = list([path_to_version[path] for path in self.all_binary_paths])
