import pytest
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture, MixedClusterFixture
from ydb.tests.oss.ydb_sdk_import import ydb


def assert_string_equal(actual, expected):
    if isinstance(actual, bytes):
        actual = actual.decode('utf-8')
    assert actual == expected


class TestTableSchemaCompatibilityRestart(RestartToAnotherVersionFixture):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_olap_schema_operations": True,
            },
            column_shard_config={
                "disabled_on_scheme_shard": False,
            },
            table_service_config={
                "enable_olap_sink": True,
            }
        )

    def test_create_table_on_one_version_read_on_another(self):
        table_name = "test_table_version_compatibility"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE TABLE `{table_name}` (
                    id Int64 NOT NULL,
                    value String,
                    PRIMARY KEY (id)
                ) WITH (
                    STORE = COLUMN
                );
            """

            session_pool.execute_with_retries(query)
            query = f"""
                UPSERT INTO `{table_name}` (id, value) VALUES (
                    1, 'test_value_1'
                ),
                (
                    2, 'test_value_2'
                )
            """

            session_pool.execute_with_retries(query)
            query = f"SELECT COUNT(*) as cnt FROM `{table_name}`"
            result_sets = session_pool.execute_with_retries(query)
            assert result_sets[0].rows[0]["cnt"] == 2

        self.change_cluster_version()
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"SELECT id, value FROM `{table_name}` ORDER BY id"
            result_sets = session_pool.execute_with_retries(query)
            assert len(result_sets[0].rows) == 2
            assert result_sets[0].rows[0]["id"] == 1
            assert_string_equal(result_sets[0].rows[0]["value"], "test_value_1")
            assert result_sets[0].rows[1]["id"] == 2
            assert_string_equal(result_sets[0].rows[1]["value"], "test_value_2")

    def test_create_alter_table_on_one_version_read_on_another(self):
        table_name = "test_table_alter_version_compatibility"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE TABLE `{table_name}` (
                    id Int64 NOT NULL,
                    value String,
                    PRIMARY KEY (id)
                ) WITH (
                    STORE = COLUMN
                );
            """

            session_pool.execute_with_retries(query)
            query = f"""
                UPSERT INTO `{table_name}` (id, value) VALUES (
                    1, 'test_value_1'
                ),
                (
                    2, 'test_value_2'
                )
            """

            session_pool.execute_with_retries(query)
            query = f"ALTER TABLE `{table_name}` ADD COLUMN new_column Int32"
            session_pool.execute_with_retries(query)
            query = f"""
                UPSERT INTO `{table_name}` (id, value, new_column) VALUES (
                    3, 'test_value_3', 42
                ),
                (
                    4, 'test_value_4', 100
                )
            """

            session_pool.execute_with_retries(query)
            query = f"SELECT COUNT(*) as cnt FROM `{table_name}`"
            result_sets = session_pool.execute_with_retries(query)
            assert result_sets[0].rows[0]["cnt"] == 4

        self.change_cluster_version()
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"SELECT id, value, new_column FROM `{table_name}` ORDER BY id"
            result_sets = session_pool.execute_with_retries(query)
            assert len(result_sets[0].rows) == 4
            assert result_sets[0].rows[0]["id"] == 1
            assert_string_equal(result_sets[0].rows[0]["value"], "test_value_1")
            assert result_sets[0].rows[0]["new_column"] is None
            assert result_sets[0].rows[2]["id"] == 3
            assert_string_equal(result_sets[0].rows[2]["value"], "test_value_3")
            assert result_sets[0].rows[2]["new_column"] == 42

    def test_create_alter_drop_columns_on_one_version_continue_on_another(self):
        table_name = "test_table_alter_drop_version_compatibility"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE TABLE `{table_name}` (
                    id Int64 NOT NULL,
                    value String,
                    PRIMARY KEY (id)
                ) WITH (
                    STORE = COLUMN
                );
            """

            session_pool.execute_with_retries(query)
            query = f"""
                UPSERT INTO `{table_name}` (id, value) VALUES (
                    1, 'test_value_1'
                ),
                (
                    2, 'test_value_2'
                )
            """

            session_pool.execute_with_retries(query)
            query = f"ALTER TABLE `{table_name}` ADD COLUMN temp_column Int32"
            session_pool.execute_with_retries(query)
            query = f"""
                UPSERT INTO `{table_name}` (id, value, temp_column) VALUES (
                    3, 'test_value_3', 42
                )
            """

            session_pool.execute_with_retries(query)
            query = f"ALTER TABLE `{table_name}` DROP COLUMN temp_column"
            session_pool.execute_with_retries(query)
            query = f"ALTER TABLE `{table_name}` ADD COLUMN final_column Double"
            session_pool.execute_with_retries(query)
            query = f"""
                UPSERT INTO `{table_name}` (id, value, final_column) VALUES (
                    4, 'test_value_4', 3.14
                )
            """

            session_pool.execute_with_retries(query)

        self.change_cluster_version()

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"ALTER TABLE `{table_name}` ADD COLUMN version_column String"
            session_pool.execute_with_retries(query)
            query = f"""
                UPSERT INTO `{table_name}` (id, value, final_column, version_column) VALUES (
                    5, 'test_value_5', 2.71, 'new_version'
                )
            """

            session_pool.execute_with_retries(query)
            query = f"SELECT id, value, final_column, version_column FROM `{table_name}` ORDER BY id"
            result_sets = session_pool.execute_with_retries(query)
            assert len(result_sets[0].rows) == 5
            assert result_sets[0].rows[0]["id"] == 1
            assert_string_equal(result_sets[0].rows[0]["value"], "test_value_1")
            assert result_sets[0].rows[0]["final_column"] is None
            assert result_sets[0].rows[0]["version_column"] is None
            assert result_sets[0].rows[4]["id"] == 5
            assert_string_equal(result_sets[0].rows[4]["value"], "test_value_5")
            assert result_sets[0].rows[4]["final_column"] == 2.71
            assert_string_equal(result_sets[0].rows[4]["version_column"], "new_version")


class TestTableSchemaCompatibilityRolling(RollingUpgradeAndDowngradeFixture):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_olap_schema_operations": True,
            },
            column_shard_config={
                "disabled_on_scheme_shard": False,
            },
            table_service_config={
                "enable_olap_sink": True,
            }
        )

    def test_table_operations_during_rolling_upgrade(self):
        table_name = "test_table_rolling_compatibility"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE TABLE `{table_name}` (
                    id Int64 NOT NULL,
                    value String,
                    PRIMARY KEY (id)
                ) WITH (
                    STORE = COLUMN
                );
            """

            session_pool.execute_with_retries(query)
            query = f"""
                UPSERT INTO `{table_name}` (id, value) VALUES (
                    1, 'initial_value_1'
                ),
                (
                    2, 'initial_value_2'
                )
            """

            session_pool.execute_with_retries(query)

        for _ in self.roll():
            with ydb.QuerySessionPool(self.driver) as session_pool:
                query = f"SELECT COUNT(*) as cnt FROM `{table_name}`"
                result_sets = session_pool.execute_with_retries(query)
                current_count = result_sets[0].rows[0]["cnt"]
                new_id = current_count + 1
                query = f"""
                    UPSERT INTO `{table_name}` (id, value) VALUES (
                        {new_id}, 'rolling_value_{new_id}'
                    )
                """

                session_pool.execute_with_retries(query)
                query = f"SELECT COUNT(*) as cnt FROM `{table_name}`"
                result_sets = session_pool.execute_with_retries(query)
                assert result_sets[0].rows[0]["cnt"] == new_id

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"ALTER TABLE `{table_name}` ADD COLUMN rolling_column Int32"
            session_pool.execute_with_retries(query)
            query = f"""
                UPSERT INTO `{table_name}` (id, value, rolling_column) VALUES (
                    999, 'final_value', 42
                )
            """

            session_pool.execute_with_retries(query)
            query = f"SELECT COUNT(*) as cnt FROM `{table_name}`"
            result_sets = session_pool.execute_with_retries(query)
            assert result_sets[0].rows[0]["cnt"] >= 10

    def test_schema_changes_during_rolling_upgrade(self):
        table_name = "test_schema_changes_rolling"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE TABLE `{table_name}` (
                    id Int64 NOT NULL,
                    value String,
                    PRIMARY KEY (id)
                ) WITH (
                    STORE = COLUMN
                );
            """

            session_pool.execute_with_retries(query)
            query = f"""
                UPSERT INTO `{table_name}` (id, value) VALUES (
                    1, 'start_value'
                )
            """

            session_pool.execute_with_retries(query)

        step_count = 0
        for _ in self.roll():
            step_count += 1
            with ydb.QuerySessionPool(self.driver) as session_pool:
                column_name = f"step_column_{step_count}"
                query = f"ALTER TABLE `{table_name}` ADD COLUMN {column_name} Int32"
                session_pool.execute_with_retries(query)
                query = f"""
                    UPSERT INTO `{table_name}` (id, value, {column_name}) VALUES (
                        {step_count + 1}, 'step_value_{step_count}', {step_count * 10}
                    )
                """

                session_pool.execute_with_retries(query)
                query = f"SELECT COUNT(*) as cnt FROM `{table_name}`"
                result_sets = session_pool.execute_with_retries(query)
                assert result_sets[0].rows[0]["cnt"] == step_count + 1

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"SELECT * FROM `{table_name}` ORDER BY id LIMIT 1"
            result_sets = session_pool.execute_with_retries(query)
            assert len(result_sets[0].columns) >= 3


class TestTableSchemaCompatibilityMixed(MixedClusterFixture):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_olap_schema_operations": True,
            },
            column_shard_config={
                "disabled_on_scheme_shard": False,
            },
            table_service_config={
                "enable_olap_sink": True,
            }
        )

    def test_table_operations_in_mixed_cluster(self):
        table_name = "test_table_mixed_cluster"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE TABLE `{table_name}` (
                    id Int64 NOT NULL,
                    value String,
                    cluster_version String,
                    PRIMARY KEY (id)
                ) WITH (
                    STORE = COLUMN
                );
            """

            session_pool.execute_with_retries(query)
            query = f"""
                UPSERT INTO `{table_name}` (id, value, cluster_version) VALUES (
                    1, 'mixed_cluster_value_1', 'mixed'
                ),
                (
                    2, 'mixed_cluster_value_2', 'mixed'
                )
            """

            session_pool.execute_with_retries(query)
            query = f"SELECT COUNT(*) as cnt FROM `{table_name}`"
            result_sets = session_pool.execute_with_retries(query)
            assert result_sets[0].rows[0]["cnt"] == 2

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"SELECT id, value FROM `{table_name}` ORDER BY id"
            result_sets = session_pool.execute_with_retries(query)
            assert len(result_sets[0].rows) == 2
            assert result_sets[0].rows[0]["id"] == 1
            assert_string_equal(result_sets[0].rows[0]["value"], "mixed_cluster_value_1")

            query = f"""
                UPSERT INTO `{table_name}` (id, value, cluster_version) VALUES (
                    3, 'mixed_cluster_value_3', 'mixed'
                )
            """

            session_pool.execute_with_retries(query)
            query = f"SELECT COUNT(*) as cnt FROM `{table_name}`"
            result_sets = session_pool.execute_with_retries(query)
            assert result_sets[0].rows[0]["cnt"] == 3

    def test_schema_changes_in_mixed_cluster(self):
        table_name = "test_schema_mixed_cluster"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE TABLE `{table_name}` (
                    id Int64 NOT NULL,
                    value String,
                    PRIMARY KEY (id)
                ) WITH (
                    STORE = COLUMN
                );
            """

            session_pool.execute_with_retries(query)
            query = f"""
                UPSERT INTO `{table_name}` (id, value) VALUES (
                    1, 'mixed_schema_value_1'
                )
            """

            session_pool.execute_with_retries(query)

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"ALTER TABLE `{table_name}` ADD COLUMN mixed_column Int32"
            session_pool.execute_with_retries(query)
            query = f"""
                UPSERT INTO `{table_name}` (id, value, mixed_column) VALUES (
                    2, 'mixed_schema_value_2', 42
                )
            """

            session_pool.execute_with_retries(query)
            query = f"SELECT id, value, mixed_column FROM `{table_name}` ORDER BY id"
            result_sets = session_pool.execute_with_retries(query)
            assert len(result_sets[0].rows) == 2
            assert result_sets[0].rows[0]["id"] == 1
            assert_string_equal(result_sets[0].rows[0]["value"], "mixed_schema_value_1")
            assert result_sets[0].rows[0]["mixed_column"] is None
            assert result_sets[0].rows[1]["id"] == 2
            assert_string_equal(result_sets[0].rows[1]["value"], "mixed_schema_value_2")
            assert result_sets[0].rows[1]["mixed_column"] == 42
