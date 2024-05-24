# -*- coding: utf-8 -*-
import logging
import os
import time
import requests

from hamcrest import (
    assert_that,
    equal_to,
    greater_than,
    not_none
)

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


class BaseDbCounters(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(
            KikimrConfigGenerator(
                additional_log_configs={
                    'SYSTEM_VIEWS': LogLevels.DEBUG
                }
            )
        )
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

    def create_table(self, driver, table):
        with ydb.SessionPool(driver, size=1) as pool:
            with pool.checkout() as session:
                session.execute_scheme(
                    "create table `{}` (key Int32, value String, primary key(key));".format(
                        table
                    )
                )

    def get_db_counters(self, service, node_index=0):
        mon_port = self.cluster.slots[node_index + 1].mon_port
        counters_url = 'http://localhost:{}/counters/counters%3D{}/json'.format(mon_port, service)
        reply = requests.get(counters_url)
        if reply.status_code == 204:
            return None

        assert_that(reply.status_code, equal_to(200))
        ret = reply.json()

        assert_that(ret, not_none())
        return ret

    def check_db_counters(self, sensors_to_check, group):
        table = os.path.join(self.database, 'table')

        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
            self.database
        )

        with ydb.Driver(driver_config) as driver:
            self.create_table(driver, table)

            with ydb.SessionPool(driver, size=1) as pool:
                with pool.checkout() as session:
                    query = "select * from `{}`".format(table)
                    session.transaction().execute(query, commit_tx=True)

            for i in range(30):
                checked = 0

                counters = self.get_db_counters('db')
                if counters:
                    sensors = counters['sensors']
                    for sensor in sensors:
                        labels = sensor['labels']
                        if labels['sensor'] in sensors_to_check:
                            assert_that(labels['group'], equal_to(group))
                            assert_that(labels['database'], equal_to(self.database))
                            assert_that(labels['host'], equal_to(''))
                            assert_that(sensor['value'], greater_than(0))
                            checked = checked + 1

                    assert_that(checked, equal_to(len(sensors_to_check)))
                    break

                if checked > 0:
                    break

                time.sleep(5)


class TestKqpCounters(BaseDbCounters):
    def test_case(self):
        sensors_to_check = {
            'Requests/Bytes',
            'Requests/QueryBytes',
            'Requests/QueryExecute',
            'YdbResponses/Success',
            'Responses/Bytes',
        }
        self.check_db_counters(sensors_to_check, 'kqp')
