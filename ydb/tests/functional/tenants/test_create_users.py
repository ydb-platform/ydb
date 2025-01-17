# -*- coding: utf-8 -*-
import logging
import os
# import time

from hamcrest import (
    anything,
    assert_that,
    # greater_than,
    has_length,
    has_properties,
)

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
# from ydb.tests.library.harness.util import LogLevels
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)

class BaseCreateUsers(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(
            KikimrConfigGenerator(
                additional_log_configs={
                    # 'SYSTEM_VIEWS': LogLevels.DEBUG
                }
            )
        )
        # cls.cluster.config.yaml_config['feature_flags']['enable_column_statistics'] = False
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def setup_method(self, method=None):
        self.database = "/Root/users/{class_name}_{method_name}".format(
            class_name=self.__class__.__name__,
            method_name=method.__name__,
        )
        logger.debug("Create database %s" % self.database)
        self.cluster.create_database(
            self.database,
            storage_pool_units_count={
                'hdd': 1
            }
        )
        self.cluster.register_and_start_slots(self.database, count=1)
        self.cluster.wait_tenant_up(self.database)

    def teardown_method(self, method=None):
        logger.debug("Remove database %s" % self.database)
        self.cluster.remove_database(self.database)
        self.database = None
       

class TestCreateUsers(BaseCreateUsers):
    def test_case(self):

        domain_admin_driver_config = ydb.DriverConfig(
            endpoint="%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
            database="/Root",
        )
        tenant_admin_driver_config = ydb.DriverConfig(
            endpoint="%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
            database=self.database,
        )
        tenant_user_driver_config = ydb.DriverConfig(
            endpoint="%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
            database=self.database,
            credentials=ydb.StaticCredentials.from_user_password("user", ""),
        )

        with ydb.Driver(domain_admin_driver_config) as driver:
            with ydb.QuerySessionPool(driver, size=1) as pool:
                pool.execute_with_retries("CREATE USER user;")

        with ydb.Driver(tenant_admin_driver_config) as driver:
            with ydb.QuerySessionPool(driver, size=1) as pool:
                pool.execute_with_retries(f"GRANT ALL ON `{self.database}` TO user;")               

        with ydb.Driver(tenant_user_driver_config) as driver:
            with ydb.QuerySessionPool(driver, size=1) as pool:
                pool.execute_with_retries("CREATE TABLE table (key Int32, value String, primary key(key));")

                pool.execute_with_retries("""UPSERT INTO table (key, value) VALUES (1, "value1");""")

                result_sets = pool.execute_with_retries("SELECT * FROM table")
                assert_that(result_sets, has_length(1))
                assert_that(result_sets[0].rows, has_length(1))
                assert_that(result_sets[0].rows[0].key == 1)
                assert_that(result_sets[0].rows[0].value == b"value1")

