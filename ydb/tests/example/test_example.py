# -*- coding: utf-8 -*-
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure

import ydb


class TestExample(object):
    """
    Example for python test
    TODO: change description to yours
    """
    @classmethod
    def setup_class(cls):
        # TODO: remove comment below
        # This cluster will be initialized before all tests in current suite and will be stopped after all tests
        # See KikimrConfigGenerator for full possibilities (feature flags, configs, erasure, etc.)
        cls.cluster = KiKiMR(KikimrConfigGenerator(erasure=Erasure.NONE))
        cls.cluster.start()

        cls.endpoint = "%s:%s" % (
            cls.cluster.nodes[1].host, cls.cluster.nodes[1].port
        )
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint=cls.endpoint
            )
        )
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        cls.driver.stop()
        cls.cluster.stop()

    def test_example(self):  # TODO: change test name to yours
        """
        Test description
        TODO: change description to yours
        """

        with ydb.QuerySessionPool(self.driver) as session_pool:
            table_name = "unique_table_name_for_test"
            value = 42

            query = f"""
                    CREATE TABLE `{table_name}` (
                    id Int64 NOT NULL,
                    value Int64 NOT NULL,
                    PRIMARY KEY (id)
                ) """
            session_pool.execute_with_retries(query)

            query = f"""UPSERT INTO `{table_name}` (id, value) VALUES (1, {value});"""
            session_pool.execute_with_retries(query)

            query = f"""SELECT id, value FROM `{table_name}` WHERE id = 1;"""
            result_sets = session_pool.execute_with_retries(query)

            assert result_sets[0].rows[0]["id"] == 1
            assert result_sets[0].rows[0]["value"] == 42

    def test_example2(self):  # TODO: change test name to yours
        """
        There can be more than 1 test in the suite
        """
        pass
