# -*- coding: utf-8 -*-
import logging

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.oss.ydb_sdk_import import ydb
import requests

logger = logging.getLogger(__name__)


class Test(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()
        cls.driver_config = ydb.DriverConfig(
            "%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port), database='/Root')
        cls.driver = ydb.Driver(cls.driver_config)
        cls.driver.wait(timeout=10)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'driver'):
            cls.driver.stop()

        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_grace_shutdown_of_session(self):
        pool = ydb.SessionPool(self.driver, size=10)
        sessions = [pool.acquire() for _ in range(10)]
        requests.get(
            'http://localhost:%s/actors/kqp_proxy?force_shutdown=all' % self.cluster.nodes[1].mon_port,
        )

        for session in sessions:
            iterations = 0
            while iterations < 10:
                try:
                    session.transaction().execute('select 1;', commit_tx=True)
                except ydb.BadSession:
                    break

                if session.closing():
                    break

                iterations += 1

            assert iterations < 10

            pool.release(session)
