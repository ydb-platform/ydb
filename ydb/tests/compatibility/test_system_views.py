# -*- coding: utf-8 -*-
import logging
import pytest
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)

pg_sysviews = {'pg_tables', 'tables', 'pg_class'}


class TestSystemViewsRegistry(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if self.versions[0] >= self.versions[1]:
            pytest.skip("Only check forward compatibility")

        yield from self.setup_cluster()

    def collect_sysviews(self):
        sysviews = dict()
        with ydb.SessionPool(self.driver, size=1) as pool:
            with pool.checkout() as session:
                for sysview in self.driver.scheme_client.list_directory("/Root/.sys").children:
                    if sysview.name not in pg_sysviews:
                        sysview_descr = dict()

                        response = session.describe_table(f"/Root/.sys/{sysview.name}")
                        columns = dict()
                        for col in response.columns:
                            columns[col.name] = str(col.type)

                        sysview_descr['columns'] = columns
                        sysview_descr['primary_key'] = [pk_col for pk_col in response.primary_key]

                        sysviews[sysview.name] = sysview_descr

        return sysviews

    def compare_sysviews_dicts(self, dict_before, dict_after):
        for sysview_name, sysview_descr in dict_before.items():
            if sysview_name not in dict_after:
                logger.debug(f"sysview '{sysview_name}' was deleted")
                return False

            columns_before = sysview_descr['columns']
            columns_after = dict_after[sysview_name]['columns']

            for col_name, col_type in columns_before.items():
                if col_name not in columns_after:
                    logger.debug(f"column '{col_name}' was deleted from sysview '{sysview_name}'")
                    return False

                if col_type != columns_after[col_name]:
                    logger.debug(f"column '{col_name}' changed type in sysview '{sysview_name}'")
                    return False

            primary_key_before = sysview_descr['primary_key']
            primary_key_after = dict_after[sysview_name]['primary_key']

            for i, primary_key_col in enumerate(primary_key_before):
                if primary_key_col != primary_key_after[i]:
                    logger.debug(f"column '{primary_key_col}' was deleted from sysview '{sysview_name}' primary key")
                    return False

        return True

    def test_domain_sys_dir(self):
        sysview_folder_content_before = self.collect_sysviews()
        self.change_cluster_version()
        sysview_folder_content_after = self.collect_sysviews()

        assert self.compare_sysviews_dicts(sysview_folder_content_before, sysview_folder_content_after)


class TestSystemViewsRollingUpgrade(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def create_table(self, table_name):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE TABLE `{table_name}` (
                    key Int32 NOT NULL,
                    value String,
                    PRIMARY KEY (key)
                );
                """
            session_pool.execute_with_retries(query)

            query = f"UPSERT INTO `{table_name}` (key, value) VALUES (1, 'test value');"
            session_pool.execute_with_retries(query)

    def read_partition_stats(self, table_name):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                SELECT PathId, PartIdx, Path
                FROM `/Root/.sys/partition_stats`
                WHERE Path = '/Root/{table_name}';"""
            result_sets = session_pool.execute_with_retries(query)

            assert len(result_sets) == 1
            assert len(result_sets[0].rows) == 1
            assert result_sets[0].rows[0]["Path"] == f'/Root/{table_name}'
            assert result_sets[0].rows[0]["PartIdx"] == 0
            assert result_sets[0].rows[0]["PathId"] > 1

    def test_path_resolving(self):
        table_name = "table"
        self.create_table(table_name)

        for _ in self.roll():
            self.read_partition_stats(table_name)
