# -*- coding: utf-8 -*-
import pytest
import logging
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


def wait_for_undetermined_ok(action, timeout_seconds=60, step_seconds=1):
    def predicate():
        try:
            action()
            return True
        except ydb.issues.Undetermined:
            return False

    return wait_for(predicate, timeout_seconds=timeout_seconds, step_seconds=step_seconds)


class TestTruncateTableRestart(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (25, 1):
            pytest.skip("Only available since 25-1")

        yield from self.setup_cluster(
            table_service_config={
                "enable_olap_sink": True,
            },
            extra_feature_flags=[
                "enable_truncate_table",
                "enable_truncate_column_table",
            ],
            column_shard_config={
                "disabled_on_scheme_shard": False,
            }
        )

    def _create_table(self, name):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                CREATE TABLE `{name}` (
                    k Uint64 NOT NULL,
                    v String,
                    PRIMARY KEY (k)
                ) WITH (
                    STORE=COLUMN
                );
            """
            )

    def _drop_table(self, name):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                DROP TABLE `{name}`
                """
            )

    def _write_to_table(self, name, start_value, count):
        data_struct_type = ydb.StructType()
        data_struct_type.add_member("k", ydb.PrimitiveType.Uint64)
        data_struct_type.add_member("v", ydb.PrimitiveType.String)
        rows = []
        for j in range(count):
            rows.append({"k": start_value + j, "v": f"Row {start_value + j} in table {name}".encode("utf-8")})
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                    DECLARE $data AS List<Struct<k: Uint64, v: String>>;
                    UPSERT INTO `{name}`
                    SELECT k AS k, v
                    FROM AS_TABLE($data);
                """,
                {"$data": (rows, ydb.ListType(data_struct_type))}
            )

    def _get_count(self, name):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            result = session_pool.execute_with_retries(
                f"""
                    SELECT COUNT(*) AS cnt FROM `{name}`;
                """
            )
            return result[0].rows[0]["cnt"]

    def _get_sum(self, name):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            result = session_pool.execute_with_retries(
                f"""
                    SELECT SUM(k) AS s FROM `{name}`;
                """
            )
            return result[0].rows[0]["s"]

    def _truncate_table(self, name):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                TRUNCATE TABLE `{name}`
                """
            )

    def test_truncate_and_restart(self):
        """Test that data written after truncate survives version change."""
        rows_per_write = 100

        # Create table and write data
        self._create_table("truncate_test")
        self._write_to_table("truncate_test", 0, rows_per_write)
        assert self._get_count("truncate_test") == rows_per_write

        # Truncate the table
        self._truncate_table("truncate_test")
        assert self._get_count("truncate_test") == 0

        # Write new data after truncation
        self._write_to_table("truncate_test", 1000, rows_per_write)
        assert self._get_count("truncate_test") == rows_per_write

        # Change cluster version
        self.change_cluster_version()

        # Verify data after version change
        assert self._get_count("truncate_test") == rows_per_write
        expected_sum = rows_per_write * (rows_per_write - 1) / 2 + 1000 * rows_per_write
        assert self._get_sum("truncate_test") == expected_sum

    def test_truncate_after_restart(self):
        """Test that truncate works after version change."""
        rows_per_write = 100

        # Create table and write data
        self._create_table("truncate_after_restart")
        self._write_to_table("truncate_after_restart", 0, rows_per_write)
        assert self._get_count("truncate_after_restart") == rows_per_write

        # Change cluster version
        self.change_cluster_version()

        # Truncate after version change
        self._truncate_table("truncate_after_restart")
        assert self._get_count("truncate_after_restart") == 0

        # Write new data and verify
        self._write_to_table("truncate_after_restart", 2000, rows_per_write)
        assert self._get_count("truncate_after_restart") == rows_per_write
        expected_sum = rows_per_write * (rows_per_write - 1) / 2 + 2000 * rows_per_write
        assert self._get_sum("truncate_after_restart") == expected_sum


class TestTruncateTableRollingUpdate(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        self.rows_per_iteration = 100

        if min(self.versions) < (25, 1):
            pytest.skip("Only available since 25-1")

        yield from self.setup_cluster(
            table_service_config={
                "enable_olap_sink": True,
            },
            extra_feature_flags=[
                "enable_truncate_table",
                "enable_truncate_column_table",
            ],
            column_shard_config={
                "disabled_on_scheme_shard": False,
            }
        )

    def _expected_sum(self, start_value, count):
        return count * (count - 1) / 2 + start_value * count

    def test_truncate_during_rolling_update(self):
        table_name = "rolling_truncate"

        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                CREATE TABLE `{table_name}` (
                    k Uint64 NOT NULL,
                    v String,
                    PRIMARY KEY (k)
                ) WITH (
                    STORE=COLUMN
                );
            """
            )

        data_struct_type = ydb.StructType()
        data_struct_type.add_member("k", ydb.PrimitiveType.Uint64)
        data_struct_type.add_member("v", ydb.PrimitiveType.String)

        iteration = 0
        for _ in self.roll():
            # Write data
            start_value = iteration * self.rows_per_iteration
            rows = []
            for j in range(self.rows_per_iteration):
                rows.append({"k": start_value + j, "v": f"Iteration {iteration}, row {j}".encode("utf-8")})

            with ydb.QuerySessionPool(self.driver) as session_pool:
                wait_for_undetermined_ok(lambda: session_pool.execute_with_retries(
                    f"""
                        DECLARE $data AS List<Struct<k: Uint64, v: String>>;
                        UPSERT INTO `{table_name}`
                        SELECT k AS k, v
                        FROM AS_TABLE($data);
                    """,
                    {"$data": (rows, ydb.ListType(data_struct_type))}
                ))

                # Verify data is present
                wait_for_undetermined_ok(lambda: session_pool.execute_with_retries(
                    f"""
                        SELECT COUNT(*) AS cnt FROM `{table_name}`;
                    """
                ))
                result = session_pool.execute_with_retries(
                    f"""
                        SELECT COUNT(*) AS cnt FROM `{table_name}`;
                    """
                )
                cnt = result[0].rows[0]["cnt"]
                logger.info(f"Iteration {iteration}: count before truncate = {cnt}")
                assert cnt > 0

                # Truncate the table
                logger.info(f"Iteration {iteration}: about to truncate")
                session_pool.execute_with_retries(
                    f"""
                        TRUNCATE TABLE `{table_name}`;
                    """
                )

                # Verify table is empty after truncation
                wait_for_undetermined_ok(lambda: session_pool.execute_with_retries(
                    f"""
                        SELECT COUNT(*) AS cnt FROM `{table_name}`;
                    """
                ))
                result = session_pool.execute_with_retries(
                    f"""
                        SELECT COUNT(*) AS cnt FROM `{table_name}`;
                    """
                )
                cnt = result[0].rows[0]["cnt"]
                logger.info(f"Iteration {iteration}: count after truncate = {cnt}")
                assert cnt == 0

            iteration += 1

        # Clean up
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                DROP TABLE `{table_name}`
            """
            )
