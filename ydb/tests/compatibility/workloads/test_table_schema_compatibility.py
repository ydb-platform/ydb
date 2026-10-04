from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest
from ydb.tests.library.compatibility.fixtures import (
    RestartToAnotherVersionFixture,
    RollingUpgradeAndDowngradeFixture,
    MixedClusterFixture,
)
from ydb.tests.oss.ydb_sdk_import import ydb


class BaseColumnTableCompatibilityTest:
    def get_column_table_config(self):
        return {
            "extra_feature_flags": ["enable_olap_schema_operations"],
            "column_shard_config": {
                "disabled_on_scheme_shard": False,
            },
            "table_service_config": {
                "enable_olap_sink": True,
            },
        }

    def create_simple_column_table(self, session_pool, table_name, additional_columns=None):
        columns = ["id Int64 NOT NULL", "value String"]
        if additional_columns:
            columns.extend(additional_columns)

        columns_str = ",\n\t\t\t".join(columns)
        query = f"""
            CREATE TABLE `{table_name}` (
                {columns_str},
                PRIMARY KEY (id)
            ) WITH (
                STORE = COLUMN
            );
        """

        session_pool.execute_with_retries(query)

    def insert_test_data(self, session_pool, table_name, data):
        values = []
        for row in data:
            row_values = []
            for _, value in row.items():
                if isinstance(value, str):
                    row_values.append(f"'{value}'")
                else:
                    row_values.append(str(value))
            values.append(f"({', '.join(row_values)})")

        columns = list(data[0].keys())
        columns_str = ", ".join(columns)
        values_str = ",\n\t\t\t".join(values)
        query = f"""
            UPSERT INTO `{table_name}` ({columns_str}) VALUES
                {values_str}
        """

        # A tablet can restart after readiness, leaving the commit outcome unknown.
        # Retrying this fixed UPSERT preserves the same keys and values.
        session_pool.execute_with_retries(query, retry_settings=ydb.RetrySettings(idempotent=True, max_retries=10))

    def get_table_count(self, session_pool, table_name):
        query = f"SELECT COUNT(*) as cnt FROM `{table_name}`"
        result_sets = session_pool.execute_with_retries(query)
        return result_sets[0].rows[0]["cnt"]

    def get_standard_test_data(self, prefix="test_value", count=2, start_id=1):
        return [{"id": start_id + i, "value": f"{prefix}_{start_id + i}"} for i in range(count)]

    def get_test_data_with_column(
        self, prefix="test_value", count=2, start_id=1, column_name="new_column", column_value=42
    ):
        return [
            {"id": start_id + i, "value": f"{prefix}_{start_id + i}", column_name: column_value + i}
            for i in range(count)
        ]

    def get_mixed_cluster_test_data(self, prefix="mixed_cluster_value", count=2, start_id=1):
        return [
            {"id": start_id + i, "value": f"{prefix}_{start_id + i}", "cluster_version": "mixed"} for i in range(count)
        ]

    def add_column_and_insert_data(self, session_pool, table_name, column_name, column_type="Int32", test_data=None):
        query = f"ALTER TABLE `{table_name}` ADD COLUMN {column_name} {column_type}"
        session_pool.execute_with_retries(query)
        if test_data:
            self.insert_test_data(session_pool, table_name, test_data)

    def verify_table_data(self, session_pool, table_name, expected_data, order_by="id"):
        columns = list(expected_data[0].keys())
        columns_str = ", ".join(columns)
        query = f"SELECT {columns_str} FROM `{table_name}` ORDER BY {order_by}"
        result_sets = session_pool.execute_with_retries(query)
        assert len(result_sets[0].rows) == len(expected_data)
        for i, expected_row in enumerate(expected_data):
            actual_row = result_sets[0].rows[i]
            for key, expected_value in expected_row.items():
                if key in actual_row:
                    if isinstance(expected_value, str):
                        assert actual_row[key] == expected_value.encode('utf-8')
                    else:
                        assert actual_row[key] == expected_value
                else:
                    pass

    def verify_table_data_compatible(self, session_pool, table_name, expected_data, order_by="id"):
        all_columns = list(expected_data[0].keys())
        available_columns = []
        for col in all_columns:
            try:
                query = f"SELECT {col} FROM `{table_name}` LIMIT 1"
                session_pool.execute_with_retries(query)
                available_columns.append(col)
            except Exception:
                pass

        if not available_columns:
            query = f"SELECT COUNT(*) as cnt FROM `{table_name}`"
            result_sets = session_pool.execute_with_retries(query)
            assert result_sets[0].rows[0]["cnt"] == len(expected_data)
            return

        columns_str = ", ".join(available_columns)
        query = f"SELECT {columns_str} FROM `{table_name}` ORDER BY {order_by}"
        result_sets = session_pool.execute_with_retries(query)
        assert len(result_sets[0].rows) == len(expected_data)
        for i, expected_row in enumerate(expected_data):
            actual_row = result_sets[0].rows[i]
            for key in available_columns:
                expected_value = expected_row[key]
                if isinstance(expected_value, str):
                    assert actual_row[key] == expected_value.encode('utf-8')
                else:
                    assert actual_row[key] == expected_value


class TestColumnTableUpsertRetries(BaseColumnTableCompatibilityTest):
    @pytest.fixture
    def pool(self, monkeypatch):
        # Exercise the real SDK retry loop without waiting between failures.
        monkeypatch.setattr("ydb.retries.time.sleep", lambda _: None)
        pool = MagicMock()
        pool._should_stop.is_set.return_value = False
        pool.execute_with_retries = lambda *args, **kwargs: ydb.QuerySessionPool.execute_with_retries(
            pool, *args, **kwargs
        )
        return pool

    def test_retries_same_upsert(self, pool):
        execute = pool.checkout.return_value.__enter__.return_value.execute
        execute.side_effect = [ydb.Undetermined("response lost"), []]

        self.insert_test_data(pool, "column_table", [{"id": 13, "value": "rolling_value_13"}])

        assert execute.call_count == 2
        assert execute.call_args_list[0] == execute.call_args_list[1]

    @pytest.mark.parametrize("error, attempts", [(ydb.Undetermined, 11), (ydb.BadRequest, 1)])
    def test_preserves_failure(self, pool, error, attempts):
        execute = pool.checkout.return_value.__enter__.return_value.execute
        execute.side_effect = error("persistent failure")

        with pytest.raises(error, match="persistent failure"):
            self.insert_test_data(pool, "column_table", [{"id": 13, "value": "rolling_value_13"}])

        assert execute.call_count == attempts
        assert all(call == execute.call_args_list[0] for call in execute.call_args_list)


class TestTableSchemaCompatibilityRestart(RestartToAnotherVersionFixture, BaseColumnTableCompatibilityTest):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        config = self.get_column_table_config()
        yield from self.setup_cluster(**config)

    def test_create_table_on_one_version_read_on_another(self):
        if min(self.versions) < (25, 1):
            pytest.skip("Test is not supported for this cluster version")

        table_name = "test_table_version_compatibility"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            self.create_simple_column_table(session_pool, table_name)
            test_data = self.get_standard_test_data()
            self.insert_test_data(session_pool, table_name, test_data)
            count = self.get_table_count(session_pool, table_name)
            assert count == 2

        self.change_cluster_version()
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"SELECT id, value FROM `{table_name}` ORDER BY id"
            result_sets = session_pool.execute_with_retries(query)
            assert len(result_sets[0].rows) == 2
            assert result_sets[0].rows[0]["id"] == 1
            assert result_sets[0].rows[0]["value"] == b"test_value_1"
            assert result_sets[0].rows[1]["id"] == 2
            assert result_sets[0].rows[1]["value"] == b"test_value_2"

    def test_create_alter_table_on_one_version_read_on_another(self):
        if min(self.versions) < (25, 1):
            pytest.skip("Test is not supported for this cluster version")

        table_name = "test_table_alter_version_compatibility"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            self.create_simple_column_table(session_pool, table_name)
            test_data = self.get_standard_test_data()
            self.insert_test_data(session_pool, table_name, test_data)
            test_data_with_new_column = self.get_test_data_with_column(start_id=3, count=2)
            self.add_column_and_insert_data(session_pool, table_name, "new_column", test_data=test_data_with_new_column)
            count = self.get_table_count(session_pool, table_name)
            assert count == 4

        self.change_cluster_version()
        with ydb.QuerySessionPool(self.driver) as session_pool:
            all_expected_data = test_data + test_data_with_new_column
            self.verify_table_data_compatible(session_pool, table_name, all_expected_data)

    def test_create_alter_drop_columns_on_one_version_continue_on_another(self):
        if min(self.versions) < (25, 1):
            pytest.skip("Test is not supported for this cluster version")

        table_name = "test_table_alter_drop_version_compatibility"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            self.create_simple_column_table(session_pool, table_name)
            test_data = self.get_standard_test_data()
            self.insert_test_data(session_pool, table_name, test_data)
            query = f"ALTER TABLE `{table_name}` ADD COLUMN temp_column Int32"
            session_pool.execute_with_retries(query)
            test_data_with_temp = [{"id": 3, "value": "test_value_3", "temp_column": 42}]
            self.insert_test_data(session_pool, table_name, test_data_with_temp)
            query = f"ALTER TABLE `{table_name}` DROP COLUMN temp_column"
            session_pool.execute_with_retries(query)
            query = f"ALTER TABLE `{table_name}` ADD COLUMN final_column Double"
            session_pool.execute_with_retries(query)
            test_data_with_final = [{"id": 4, "value": "test_value_4", "final_column": 3.14}]
            self.insert_test_data(session_pool, table_name, test_data_with_final)

        self.change_cluster_version()

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"ALTER TABLE `{table_name}` ADD COLUMN version_column String"
            session_pool.execute_with_retries(query)
            test_data_with_version = [
                {"id": 5, "value": "test_value_5", "final_column": 2.71, "version_column": "new_version"}
            ]
            self.insert_test_data(session_pool, table_name, test_data_with_version)
            query = f"SELECT id, value, final_column, version_column FROM `{table_name}` ORDER BY id"
            result_sets = session_pool.execute_with_retries(query)
            assert len(result_sets[0].rows) == 5
            assert result_sets[0].rows[0]["id"] == 1
            assert result_sets[0].rows[0]["value"] == b"test_value_1"
            assert result_sets[0].rows[0]["final_column"] is None
            assert result_sets[0].rows[0]["version_column"] is None
            assert result_sets[0].rows[4]["id"] == 5
            assert result_sets[0].rows[4]["value"] == b"test_value_5"
            assert result_sets[0].rows[4]["final_column"] == 2.71
            assert result_sets[0].rows[4]["version_column"] == b"new_version"


class TestTableSchemaCompatibilityRolling(RollingUpgradeAndDowngradeFixture, BaseColumnTableCompatibilityTest):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        config = self.get_column_table_config()
        yield from self.setup_cluster(**config)

    def test_upsert_after_lost_commit_response(self, monkeypatch):
        if min(self.versions) < (25, 1):
            pytest.skip("Test is not supported for this cluster version")

        table_name = "test_upsert_lost_response"
        expected = self.get_standard_test_data(prefix="committed_value")
        with ydb.QuerySessionPool(self.driver) as session_pool:
            self.create_simple_column_table(session_pool, table_name)
            checkout = session_pool.checkout
            requests = []

            @contextmanager
            def checkout_with_lost_response(*args, **kwargs):
                with checkout(*args, **kwargs) as session:
                    execute = session.execute

                    def lose_first_response(query, *args, **kwargs):
                        requests.append(query)
                        result = list(execute(query, *args, **kwargs))
                        # The server has completed the write before its response is lost.
                        if len(requests) == 1:
                            raise ydb.Undetermined("injected loss after commit")
                        return iter(result)

                    with monkeypatch.context() as patch:
                        patch.setattr(session, "execute", lose_first_response)
                        yield session

            with monkeypatch.context() as patch:
                patch.setattr(session_pool, "checkout", checkout_with_lost_response)
                self.insert_test_data(session_pool, table_name, expected)

            assert len(requests) == 2
            assert requests[0] == requests[1]
            self.verify_table_data(session_pool, table_name, expected)
            assert self.get_table_count(session_pool, table_name) == len(expected)

    def test_table_operations_during_rolling_upgrade(self):
        if min(self.versions) < (25, 1):
            pytest.skip("Test is not supported for this cluster version")

        table_name = "test_table_rolling_compatibility"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            self.create_simple_column_table(session_pool, table_name)
            test_data = self.get_standard_test_data(prefix="initial_value")
            self.insert_test_data(session_pool, table_name, test_data)
            expected_data = list(test_data)

        for _ in self.roll():
            with ydb.QuerySessionPool(self.driver) as session_pool:
                current_count = self.get_table_count(session_pool, table_name)
                new_id = current_count + 1
                test_data = [{"id": new_id, "value": f"rolling_value_{new_id}"}]
                self.insert_test_data(session_pool, table_name, test_data)
                count = self.get_table_count(session_pool, table_name)
                assert count == new_id
                expected_data.extend(test_data)
                self.verify_table_data(session_pool, table_name, expected_data)

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"ALTER TABLE `{table_name}` ADD COLUMN rolling_column Int32"
            session_pool.execute_with_retries(query)
            test_data = [{"id": 999, "value": "final_value", "rolling_column": 42}]
            self.insert_test_data(session_pool, table_name, test_data)
            count = self.get_table_count(session_pool, table_name)
            assert count >= 10
            expected_data.extend(test_data)
            self.verify_table_data(session_pool, table_name, expected_data)
            result = session_pool.execute_with_retries(f"SELECT rolling_column FROM `{table_name}` WHERE id = 999")
            assert result[0].rows[0]["rolling_column"] == 42

    def test_schema_changes_during_rolling_upgrade(self):
        if min(self.versions) < (25, 1):
            pytest.skip("Test is not supported for this cluster version")

        table_name = "test_schema_changes_rolling"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            self.create_simple_column_table(session_pool, table_name)
            test_data = [{"id": 1, "value": "start_value"}]
            self.insert_test_data(session_pool, table_name, test_data)

        step_count = 0
        for _ in self.roll():
            step_count += 1
            with ydb.QuerySessionPool(self.driver) as session_pool:
                column_name = f"step_column_{step_count}"
                query = f"ALTER TABLE `{table_name}` ADD COLUMN {column_name} Int32"
                session_pool.execute_with_retries(query)
                test_data = [{"id": step_count + 1, "value": f"step_value_{step_count}", column_name: step_count * 10}]

                self.insert_test_data(session_pool, table_name, test_data)
                count = self.get_table_count(session_pool, table_name)
                assert count == step_count + 1
                result = session_pool.execute_with_retries(
                    f"SELECT value, {column_name} FROM `{table_name}` WHERE id = {step_count + 1}"
                )
                assert len(result[0].rows) == 1
                assert result[0].rows[0]["value"] == f"step_value_{step_count}".encode("utf-8")
                assert result[0].rows[0][column_name] == step_count * 10

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"SELECT * FROM `{table_name}` ORDER BY id LIMIT 1"
            result_sets = session_pool.execute_with_retries(query)
            assert len(result_sets[0].columns) >= 3


class TestTableSchemaCompatibilityMixed(MixedClusterFixture, BaseColumnTableCompatibilityTest):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        config = self.get_column_table_config()
        yield from self.setup_cluster(**config)

    def test_table_operations_in_mixed_cluster(self):
        if min(self.versions) < (25, 1):
            pytest.skip("Test is not supported for this cluster version")

        table_name = "test_table_mixed_cluster"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            self.create_simple_column_table(session_pool, table_name, ["cluster_version String"])
            test_data = self.get_mixed_cluster_test_data()
            self.insert_test_data(session_pool, table_name, test_data)
            count = self.get_table_count(session_pool, table_name)
            assert count == 2

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"SELECT id, value FROM `{table_name}` ORDER BY id"
            result_sets = session_pool.execute_with_retries(query)
            assert len(result_sets[0].rows) == 2
            assert result_sets[0].rows[0]["id"] == 1
            assert result_sets[0].rows[0]["value"] == b"mixed_cluster_value_1"
            test_data = [{"id": 3, "value": "mixed_cluster_value_3", "cluster_version": "mixed"}]
            self.insert_test_data(session_pool, table_name, test_data)
            count = self.get_table_count(session_pool, table_name)
            assert count == 3

    def test_schema_changes_in_mixed_cluster(self):
        if min(self.versions) < (25, 1):
            pytest.skip("Test is not supported for this cluster version")

        table_name = "test_schema_mixed_cluster"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            self.create_simple_column_table(session_pool, table_name)
            test_data = [{"id": 1, "value": "mixed_schema_value_1"}]
            self.insert_test_data(session_pool, table_name, test_data)

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"ALTER TABLE `{table_name}` ADD COLUMN mixed_column Int32"
            session_pool.execute_with_retries(query)
            test_data = [{"id": 2, "value": "mixed_schema_value_2", "mixed_column": 42}]
            self.insert_test_data(session_pool, table_name, test_data)
            query = f"SELECT id, value, mixed_column FROM `{table_name}` ORDER BY id"
            result_sets = session_pool.execute_with_retries(query)
            assert len(result_sets[0].rows) == 2
            assert result_sets[0].rows[0]["id"] == 1
            assert result_sets[0].rows[0]["value"] == b"mixed_schema_value_1"
            assert result_sets[0].rows[0]["mixed_column"] is None
            assert result_sets[0].rows[1]["id"] == 2
            assert result_sets[0].rows[1]["value"] == b"mixed_schema_value_2"
            assert result_sets[0].rows[1]["mixed_column"] == 42
