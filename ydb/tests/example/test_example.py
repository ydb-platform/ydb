# -*- coding: utf-8 -*-
import pytest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.test_meta import skip_with_issue, link_test_case

import ydb


class TestExample:
    """
    Example for python test
    TODO: change description to yours
    """
    @pytest.fixture(autouse=True)
    def setup(self):
        # TODO: remove comment below
        # This cluster will be initialized before all tests in current suite and will be stopped after all tests
        # See KikimrConfigGenerator for full possibilities (feature flags, configs, erasure, etc.)
        self.cluster = KiKiMR(KikimrConfigGenerator(erasure=Erasure.NONE))
        self.cluster.start()

        self.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint=self.cluster.nodes[1].endpoint,
            )
        )
        self.driver.wait()

        yield

        self.driver.stop()
        self.cluster.stop()

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

    @link_test_case("#999999998")  # test can be linked with test-case issue in github
    def test_linked_with_testcase(self):
        pass

    @skip_with_issue("#999999999")  # test can be skipped with github issue
    def test_skipped_with_issue(self):
        pass
