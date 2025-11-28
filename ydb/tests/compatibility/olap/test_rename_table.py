# -*- coding: utf-8 -*-
import pytest
import logging
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture, RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


def wait_for_undetermined_ok(action, timeout_seconds=60, step_seconds=1):
    def predicate():
        try:
            action()
            return True
        except ydb.issues.Undetermined:
            return False
        except ydb.issues.SchemeError as e:
            error_msg = str(e).lower()
            if "cannot find table" in error_msg or "does not exist" in error_msg:
                logger.debug(f"Got temporary SchemeError, will retry: {e}")
                return False
            raise

    return wait_for(predicate, timeout_seconds=timeout_seconds, step_seconds=step_seconds)


class TestRenameTableRestart(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):

        if min(self.versions) < (25, 1):
            pytest.skip("Only available since 25-1")

        yield from self.setup_cluster(table_service_config={
            "enable_olap_sink": True,
        }, extra_feature_flags={
            "enable_move_column_table": True,
        }, column_shard_config={
            "disabled_on_scheme_shard": False,
            "generate_internal_path_id": True
        })

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

    def _rename_table(self, from_name, to_name):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                ALTER TABLE {from_name} RENAME TO {to_name}
            """
            )

    def _drop_table(self, name):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                DROP TABLE {name}
                """
            )

    def _write_to_table(self, name, start_value, count):
        data_struct_type = ydb.StructType()
        data_struct_type.add_member("k", ydb.PrimitiveType.Uint64)
        data_struct_type.add_member("v", ydb.PrimitiveType.String)
        rows = []
        for j in range(count):
            rows.append({"k": start_value + j, "v": f"Write row {j} to table {name}".encode("utf-8")})
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

    def _get_sum(self, name):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            result = session_pool.execute_with_retries(
                f"""
                    select sum(k) as sum from `{name}`;
                """
            )
            return result[0].rows[0]["sum"]

    def _expected_sum(self, start_value, count):
        return count * (count - 1) / 2 + start_value * count

    def test_rename_table(self):
        rows_per_write = 100

        self._create_table("table1")
        table1_start_value = 0
        self._write_to_table("table1", table1_start_value, rows_per_write)
        self._rename_table("table1", "renamed1")

        self._create_table("table2")
        table2_start_value = 1000
        self._write_to_table("table2", table2_start_value, rows_per_write)

        self.change_cluster_version()

        # check that renamed table is working
        self._write_to_table("renamed1", table1_start_value + rows_per_write, rows_per_write)
        assert self._expected_sum(table1_start_value, rows_per_write * 2) == self._get_sum("renamed1")

        # check that created table can be renamed
        self._rename_table("table2", "renamed2")
        self._write_to_table("renamed2", table2_start_value + rows_per_write, rows_per_write)
        assert self._expected_sum(table2_start_value, rows_per_write * 2) == self._get_sum("renamed2")


class TestRenameTableRollingUpdate(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        self.rows_per_iteration = 100

        if min(self.versions) < (25, 1):
            pytest.skip("Only available since 25-1")

        yield from self.setup_cluster(table_service_config={
            "enable_olap_sink": True,
        }, extra_feature_flags={
            "enable_move_column_table": True,
        }, column_shard_config={
            "disabled_on_scheme_shard": False,
            "generate_internal_path_id": True
        })

    def get_table_name(self, i):
        return f"table_{i}"

    def run_iteration(self, iteration):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            rows = []
            for j in range(self.rows_per_iteration):
                rows.append({"k": iteration * self.rows_per_iteration + j, "v": f"Iteration {iteration}, row {j}".encode("utf-8")})
            data_struct_type = ydb.StructType()
            data_struct_type.add_member("k", ydb.PrimitiveType.Uint64)
            data_struct_type.add_member("v", ydb.PrimitiveType.String)

            wait_for_undetermined_ok(lambda: session_pool.execute_with_retries(
                f"""
                    DECLARE $data AS List<Struct<k: Uint64, v: String>>;
                    UPSERT INTO `{self.get_table_name(iteration)}`
                    SELECT k AS k, v
                    FROM AS_TABLE($data);
                """,
                {"$data": (rows, ydb.ListType(data_struct_type))}
            ))
            logger.info(f"Iteration {iteration} about to rename table_{iteration} to table_{iteration + 1}")
            wait_for_undetermined_ok(lambda: session_pool.execute_with_retries(
                f"""
                    select count(*) as cnt from `{self.get_table_name(iteration)}`;
                """
            ))
            session_pool.execute_with_retries(
                f"""
                    ALTER TABLE `{self.get_table_name(iteration)}` RENAME TO `{self.get_table_name(iteration + 1)}`;
                """
            )
            wait_for_undetermined_ok(lambda: session_pool.execute_with_retries(
                f"""
                    select count(*) as cnt from `{self.get_table_name(iteration + 1)}`;
                """
            ))
            result = session_pool.execute_with_retries(
                f"""
                    select count(*) as cnt from `{self.get_table_name(iteration + 1)}`;
                """
            )
            cnt = result[0].rows[0]["cnt"]
            assert cnt == (iteration + 1) * self.rows_per_iteration

    def test_rename_table(self):
        iteration = 0
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                CREATE TABLE `{self.get_table_name(iteration)}` (
                    k Uint64 NOT NULL,
                    v String,
                    PRIMARY KEY (k)
                ) WITH (
                    STORE=COLUMN
                );
            """
            )

        for _ in self.roll():
            self.run_iteration(iteration)
            iteration += 1

        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                DROP TABLE `{self.get_table_name(iteration)}`
            """
            )
