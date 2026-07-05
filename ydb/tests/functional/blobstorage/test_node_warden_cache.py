# -*- coding: utf-8 -*-
import logging
import os
import shutil
import stat
import tempfile

import pytest
import requests

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


class CacheTestBase(object):

    erasure = Erasure.NONE
    nodes_count = 1

    def start_cluster(self, cache_file_path):
        configurator = KikimrConfigGenerator(
            erasure=self.erasure,
            nodes=self.nodes_count,
            use_in_memory_pdisks=False,
            bs_cache_file_path=cache_file_path,
        )
        self.cluster = KiKiMR(configurator)
        self.cluster.start()

    def get_cache_write_error_counter(self, node):
        url = "http://localhost:%d/counters/counters=config/json" % node.mon_port
        try:
            response = requests.get(url, timeout=5)
        except Exception as e:
            logger.warning("failed to read counters from node: %s", e)
            return None
        if response.status_code != 200:
            return None
        for sensor in response.json().get("sensors", []):
            if sensor.get("labels", {}).get("sensor") == "CacheFileWriteError":
                return sensor.get("value")
        return None

    def assert_cluster_operational(self):
        driver = ydb.Driver(endpoint=self.cluster.nodes[1].endpoint, database='/Root')
        driver.wait(timeout=60, fail_fast=True)
        try:
            with ydb.QuerySessionPool(driver) as pool:
                rows = pool.execute_with_retries("SELECT 1 AS value")[0].rows
                assert rows[0]["value"] == 1
        finally:
            driver.stop()


class TestNodeWardenCacheFile(CacheTestBase):

    @pytest.fixture(autouse=True)
    def setup(self):
        self.cache_root = tempfile.mkdtemp(prefix="node_warden_cache_")
        self.cache_dir = os.path.join(self.cache_root, "auto", "created", "subdir")
        cache_file_path = os.path.join(self.cache_dir, "node_warden_cache_%n.txt")

        assert not os.path.exists(self.cache_dir), "cache directory must not exist before the cluster starts"

        self.start_cluster(cache_file_path)

        yield

        self.cluster.stop()
        shutil.rmtree(self.cache_root, ignore_errors=True)

    def cache_file_for_node(self, node_id):
        return os.path.join(self.cache_dir, "node_warden_cache_%d.txt" % node_id)

    def all_cache_files_present(self):
        return all(os.path.exists(self.cache_file_for_node(node_id)) for node_id in self.cluster.nodes)

    def all_writes_ok(self):
        return all(self.get_cache_write_error_counter(node) == 0 for node in self.cluster.nodes.values())

    def test_cache_file_lifecycle_and_restart(self):
        self.assert_cluster_operational()

        assert wait_for(self.all_cache_files_present, timeout_seconds=120), "failed to create the cache files (and their directory)"
        assert os.path.isdir(self.cache_dir), "ydbd was expected to create the cache directory"

        for node_id in self.cluster.nodes:
            mode = stat.S_IMODE(os.stat(self.cache_file_for_node(node_id)).st_mode)
            assert mode == 0o644, "cache file must be 0644 (owner rw, group/other read-only), got %o" % mode

        assert wait_for(self.all_writes_ok, timeout_seconds=120), "config/CacheFileWriteError metric did not become 0 on all nodes"

        for node in self.cluster.nodes.values():
            node.stop()
        for node_id in self.cluster.nodes:
            assert os.path.exists(self.cache_file_for_node(node_id)), "cache file disappeared while the node was stopped"
        for node in self.cluster.nodes.values():
            node.start()

        self.assert_cluster_operational()
        assert wait_for(self.all_writes_ok, timeout_seconds=120), "config/CacheFileWriteError metric did not become 0 after the restart"


class TestNodeWardenCacheFileWriteError(CacheTestBase):

    @pytest.fixture(autouse=True)
    def setup(self):
        self.cache_root = tempfile.mkdtemp(prefix="node_warden_cache_err_")
        self.blocking_file = os.path.join(self.cache_root, "not_a_dir")
        with open(self.blocking_file, "w"):
            pass
        cache_file_path = os.path.join(self.blocking_file, "node_warden_cache_%n.txt")

        self.start_cluster(cache_file_path)

        yield

        self.cluster.stop()
        shutil.rmtree(self.cache_root, ignore_errors=True)

    def all_writes_failed(self):
        return all(self.get_cache_write_error_counter(node) == 1 for node in self.cluster.nodes.values())

    def test_write_error_metric_on_unwritable_path(self):
        self.assert_cluster_operational()

        assert wait_for(self.all_writes_failed, timeout_seconds=120), "config/CacheFileWriteError metric did not become 1 on all nodes"

        assert os.path.isfile(self.blocking_file), "the blocking file must stay an untouched regular file"
