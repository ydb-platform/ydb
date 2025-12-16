# -*- coding: utf-8 -*-
import pytest
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestShowCreateTable(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (25, 3, 1):
            pytest.skip("compatibility for show create table is not supported in < 25.3.1")
        self.table_name = "test_show_create_table"
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_show_create": True,
            }
        )

    def create_table(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(f"""
                CREATE TABLE `{self.table_name}` (
                    key Int32,
                    value Utf8,
                    PRIMARY KEY (key)
                );
            """)

    def show_create_table(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            result_sets = session_pool.execute_with_retries(f"SHOW CREATE TABLE `{self.table_name}`;")

            if not (result_sets and result_sets[0].rows and result_sets[0].rows[0]):
                raise AssertionError(f"SHOW CREATE TABLE `{self.table_name}` returned no data.")

            create_query_column_index = -1
            for i, col in enumerate(result_sets[0].columns):
                if col.name == "CreateQuery":
                    create_query_column_index = i
                    break

            if create_query_column_index == -1:
                raise AssertionError(f"Column 'CreateQuery' not found in SHOW CREATE TABLE result for {self.table_name}.")

            create_query = result_sets[0].rows[0][create_query_column_index]

            # Validate the CREATE TABLE query contains expected elements
            expected_table_substr = f"CREATE TABLE `{self.table_name}`"
            expected_key_substr = "`key` Int32"
            expected_value_substr = "`value` Utf8"
            expected_primary_key_substr = "PRIMARY KEY (`key`)"

            assert expected_table_substr in create_query, f"Expected '{expected_table_substr}' in CREATE TABLE query"
            assert expected_key_substr in create_query, f"Expected '{expected_key_substr}' in CREATE TABLE query"
            assert expected_value_substr in create_query, f"Expected '{expected_value_substr}' in CREATE TABLE query"
            assert expected_primary_key_substr in create_query, f"Expected '{expected_primary_key_substr}' in CREATE TABLE query"

            return create_query

    def test_show_create_table_after_version_change(self):
        # Create table before version change
        self.create_table()

        # Test SHOW CREATE TABLE before version change
        create_query_before = self.show_create_table()

        # Change cluster version
        self.change_cluster_version()

        # Test SHOW CREATE TABLE after version change
        create_query_after = self.show_create_table()

        # Verify the CREATE TABLE queries are consistent
        assert create_query_before == create_query_after, "CREATE TABLE query should be consistent after version change"
