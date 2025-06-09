# -*- coding: utf-8 -*-
import pytest
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture, MixedClusterFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestExampleMixedClusterFixture(MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        #
        # Setup cluster
        #
        yield from self.setup_cluster(
            # # Some feature flags can be passed. And other KikimrConfigGenerator options
            # extra_feature_flags={
            #     "some_feature_flag": True,
            # }
        )

    def test_example(self):
        #
        # 1. Fill table with data
        #
        table_name = "table_name"
        value = 42
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                    CREATE TABLE `{table_name}` (
                    id Int64 NOT NULL,
                    value Int64 NOT NULL,
                    PRIMARY KEY (id)
                ) """
            session_pool.execute_with_retries(query)

            query = f"""UPSERT INTO `{table_name}` (id, value) VALUES (1, {value});"""
            session_pool.execute_with_retries(query)

        #
        # 2. check written data is correct
        #
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""SELECT id, value FROM `{table_name}` WHERE id = 1;"""
            result_sets = session_pool.execute_with_retries(query)

            assert result_sets[0].rows[0]["id"] == 1
            assert result_sets[0].rows[0]["value"] == value


class TestExampleRestartToAnotherVersion(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        #
        # Setup cluster
        #
        yield from self.setup_cluster(
            # # Some feature flags can be passed. And other KikimrConfigGenerator options
            # extra_feature_flags={
            #     "some_feature_flag": True,
            # }
        )

    def test_example(self):
        #
        # 1. Fill table with data
        #
        table_name = "table_name"
        value = 42
        with ydb.QuerySessionPool(self.driver) as session_pool:
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
            assert result_sets[0].rows[0]["value"] == value

        #
        # 2. stop the cluster, and start using another version
        #
        self.change_cluster_version()

        #
        # 3. check written data is correct
        #
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""SELECT id, value FROM `{table_name}` WHERE id = 1;"""
            result_sets = session_pool.execute_with_retries(query)

            assert result_sets[0].rows[0]["id"] == 1
            assert result_sets[0].rows[0]["value"] == value


class TestExampleRollingUpdate(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        #
        # Setup cluster
        #
        yield from self.setup_cluster(
            # # Some feature flags can be passed. And other KikimrConfigGenerator options
            # extra_feature_flags={
            #     "some_feature_flag": True,
            # }
        )

    def test_example(self):
        table_name = "table_name"
        value = 42

        #
        # 1. Fill table with data
        #
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                    CREATE TABLE `{table_name}` (
                    id Int64 NOT NULL,
                    value Int64 NOT NULL,
                    ts Timestamp,
                    PRIMARY KEY (id)
                ) """
            session_pool.execute_with_retries(query)

            query = f"""UPSERT INTO `{table_name}` (id, value) VALUES (1, {value});"""
            session_pool.execute_with_retries(query)

        for _ in self.roll():  # every iteration is a step in rolling upgrade process
            #
            # 2. check written data is correct during rolling upgrade
            #
            with ydb.QuerySessionPool(self.driver) as session_pool:
                query = f"""SELECT id, value FROM `{table_name}` WHERE id = 1;"""
                result_sets = session_pool.execute_with_retries(query)

                assert result_sets[0].rows[0]["id"] == 1
                assert result_sets[0].rows[0]["value"] == value
