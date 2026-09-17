# -*- coding: utf-8 -*-
"""
YDBBUGS-872: rolling fixture used only nodes[1] as the driver seed.

After the bootstrap node is stopped, the old create_driver() times out on
endpoints[0]. The new helper seeds discovery from remaining live nodes.
"""
import logging
import unittest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.compatibility.driver_seeds import create_ydb_driver
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)

DRIVER_WAIT_TIMEOUT_SECONDS = 15


class TestCreateDriverAfterBootstrapNodeStop(unittest.TestCase):
    def setUp(self):
        self.cluster = KiKiMR(
            KikimrConfigGenerator(
                erasure=Erasure.NONE,
                nodes=3,
                n_to_select=1,
                extra_feature_flags=["suppress_compatibility_check"],
            )
        )
        self.cluster.start()
        self.database_path = "/Root"

    def tearDown(self):
        if hasattr(self, "cluster"):
            self.cluster.stop()

    def _query_select_one(self, driver):
        with ydb.QuerySessionPool(driver) as session_pool:
            result_sets = session_pool.execute_with_retries("SELECT 1")
            self.assertEqual(result_sets[0].rows[0][0], 1)

    def test_create_driver_after_bootstrap_node_stop(self):
        driver = create_ydb_driver(self.database_path, self.cluster, timeout=DRIVER_WAIT_TIMEOUT_SECONDS)
        try:
            self._query_select_one(driver)
        finally:
            driver.stop()

        bootstrap = self.cluster.nodes[1]
        bootstrap.stop()
        self.assertFalse(bootstrap.is_alive())
        logger.info("Stopped bootstrap node %s", bootstrap)

        driver = create_ydb_driver(self.database_path, self.cluster, timeout=DRIVER_WAIT_TIMEOUT_SECONDS)
        try:
            self._query_select_one(driver)
        finally:
            driver.stop()
