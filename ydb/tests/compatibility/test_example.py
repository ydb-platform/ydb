# -*- coding: utf-8 -*-
import pytest
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture, RollingUpdateFixture, MixedClusterFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestExampleMixedClusterFixture(MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_example(self):
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


class TestExampleRestartToAnotherVersion(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_example(self):
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

        self.change_cluster_version()  # change version to another

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""SELECT id, value FROM `{table_name}` WHERE id = 1;"""
            result_sets = session_pool.execute_with_retries(query)

            assert result_sets[0].rows[0]["id"] == 1
            assert result_sets[0].rows[0]["value"] == value


class TestExampleRollingUpdate(RollingUpdateFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_example(self):
        table_name = "table_name"
        value = 42

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

        for _ in self.roll():
            with ydb.QuerySessionPool(self.driver) as session_pool:
                query = f"""SELECT id, value FROM `{table_name}` WHERE id = 1;"""
                result_sets = session_pool.execute_with_retries(query)

                assert result_sets[0].rows[0]["id"] == 1
                assert result_sets[0].rows[0]["value"] == value
