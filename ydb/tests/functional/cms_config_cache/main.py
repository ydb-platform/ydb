#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import time

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

logger = logging.getLogger(__name__)


class TestCmsConfigCacheMain(object):

    sample_log_config = """
LogConfig {
  Entry {
    Component: "FLAT_TX_SCHEMESHARD"
    Level: 7
  }
  Entry {
    Component: "TENANT_SLOT_BROKER"
    Level: 7
  }
  Entry {
    Component: "TX_DATASHARD"
    Level: 5
  }
  Entry {
    Component: "LOCAL"
    Level: 7
  }
  Entry {
    Component: "TX_PROXY"
    Level: 5
  }
  Entry {
    Component: "HIVE"
    Level: 7
  }
  Entry {
    Component: "BS_CONTROLLER"
    Level: 7
  }
  SysLog: true
}
"""

    log_entry = """Entry {
    Component: "FLAT_TX_SCHEMESHARD"
    Level: 7
  }"""

    @classmethod
    def setup_class(cls):
        configurator = KikimrConfigGenerator()
        cls.cluster = kikimr_cluster_factory(configurator=configurator)
        cls.cluster.start()
        cls.cluster.create_database(
            '/Root/database',
            storage_pool_units_count={'hdd': 1}
        )
        cls.cluster.register_and_start_slots('/Root/database', count=1)
        cls.cluster.wait_tenant_up('/Root/database')

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def _cms_config_cache_file_path(self):
        slot = self.cluster.slots[1]
        path = os.path.join(slot.cwd, slot.cms_config_cache_file_name)
        return path

    def test_cache_log_settings(self):
        with open(self._cms_config_cache_file_path(), 'r') as file:
            assert self.log_entry not in file.read(), "initial node config should not contain LogConfig items"
        self.cluster.client.add_config_item(self.sample_log_config)
        timeout = 60
        step = 1
        cur = 0
        config_updated = False
        while not config_updated and cur < timeout:
            time.sleep(step)
            cur += step
            with open(self._cms_config_cache_file_path(), 'r') as file:
                config_updated = self.log_entry in file.read()
        assert config_updated, "log config wasn't updated"

    def test_cms_config_cache(self):
        with open(self._cms_config_cache_file_path(), 'r') as file:
            cfg = file.read()
            assert len(cfg) > 0
