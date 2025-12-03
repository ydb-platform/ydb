# -*- coding: utf-8 -*-
import logging
import pytest
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)

pg_sysviews = {'pg_tables', 'tables', 'pg_class'}


class TestSystemViewsRegistry(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope='function')
    def setup(self):
        if self.versions[0] >= self.versions[1]:
            pytest.skip('Only check forward compatibility')

        yield from self.setup_cluster()

    def collect_sysviews(self):
        sysviews = dict()
        with ydb.SessionPool(self.driver, size=1) as pool:
            with pool.checkout() as session:
                for sysview in self.driver.scheme_client.list_directory('/Root/.sys').children:
                    sysview_descr = dict()

                    try:
                        response = session.describe_table(f'/Root/.sys/{sysview.name}')
                    except TypeError:
                        if sysview.name in pg_sysviews:
                            continue
                        else:
                            raise

                    columns = dict()
                    for col in response.columns:
                        columns[col.name] = str(col.type)

                    sysview_descr['columns'] = columns
                    sysview_descr['primary_key'] = [pk_col for pk_col in response.primary_key]

                    sysviews[sysview.name] = sysview_descr

        return sysviews

    def compare_sysviews_dicts(self, dict_before, dict_after):
        for sysview_name, sysview_descr in dict_before.items():
            if min(self.versions) < (25, 1, 4) and sysview_name in ['resource_pools', 'resource_pool_classifiers']:
                continue
            if sysview_name not in dict_after:
                return False, f"sysview '{sysview_name}' was deleted"

            columns_before = sysview_descr['columns']
            columns_after = dict_after[sysview_name]['columns']

            for col_name, col_type in columns_before.items():
                if col_name not in columns_after:
                    return False, f"column '{col_name}' was deleted from sysview '{sysview_name}'"

                if col_type != columns_after[col_name]:
                    return False, f"column '{col_name}' changed type in sysview '{sysview_name}'"

            primary_key_before = sysview_descr['primary_key']
            primary_key_after = dict_after[sysview_name]['primary_key']

            for i, primary_key_col in enumerate(primary_key_before):
                if primary_key_col != primary_key_after[i]:
                    return False, f"column '{primary_key_col}' was deleted from sysview '{sysview_name}' primary key"

        return True, ''

    def test_domain_sys_dir(self):
        sysview_folder_content_before = self.collect_sysviews()
        self.change_cluster_version()
        sysview_folder_content_after = self.collect_sysviews()

        result, debug_compare_string = self.compare_sysviews_dicts(sysview_folder_content_before, sysview_folder_content_after)
        assert result, debug_compare_string


class TestSystemViewsRollingUpgrade(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope='function')
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
            assert result_sets[0].rows[0]['Path'] == f'/Root/{table_name}'
            assert result_sets[0].rows[0]['PartIdx'] == 0
            assert result_sets[0].rows[0]['PathId'] > 1

    def test_path_resolving(self):
        table_name = 'table'
        self.create_table(table_name)

        for _ in self.roll():
            self.read_partition_stats(table_name)


class TestSystemViewsSetPermissions(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope='function')
    def setup(self):
        if min(self.versions) < (25, 3):
            pytest.skip('Only available since 25-3')

        yield from self.setup_cluster(
            extra_feature_flags={
                'enable_real_system_view_paths': True,
            }
        )

    def permissions_to_dict(self, permissions):
        permissions_dict = dict()
        for permission in permissions:
            if permission.subject not in permissions_dict:
                permissions_dict[permission.subject] = list(permission.permission_names)
            else:
                permissions_dict[permission.subject].extend(list(permission.permission_names))

        return permissions_dict

    def sort_permissions(self, permissions_dict):
        for subject in permissions_dict:
            permissions_dict[subject] = sorted(permissions_dict[subject])

    def compare_permissions_dicts(self, permissions_before, permissions_after):
        for subject in permissions_before:
            if subject not in permissions_after:
                return False, f"Subject '{subject}' is missing in permissions after version change"

            if permissions_before[subject] != permissions_after[subject]:
                return (
                    False,
                    f"Permissions for subject '{subject}' changed: "
                    f"was {permissions_before[subject]}, became {permissions_after[subject]}"
                )

        for subject in permissions_after:
            if subject not in permissions_before:
                return (
                    False,
                    f"New subject '{subject}' appeared with permissions {permissions_after[subject]}"
                )

        return True, ''

    def set_permissions(self, permissions):
        permissions_settings = ydb.ModifyPermissionsSettings()
        for user, permissions in permissions.items():
            permissions_settings.grant_permissions(user, permissions)

        self.driver.scheme_client.modify_permissions('/Root/.sys', permissions_settings)
        self.driver.scheme_client.modify_permissions('/Root/.sys/partition_stats', permissions_settings)

    def check_permissions(self, permissions):
        self.sort_permissions(permissions)

        sys_dir_desc = self.driver.scheme_client.describe_path('/Root/.sys')
        sys_dir_permissions = self.permissions_to_dict(sys_dir_desc.permissions)
        self.sort_permissions(sys_dir_permissions)
        result, debug_compare_string = self.compare_permissions_dicts(permissions, sys_dir_permissions)
        assert result, debug_compare_string

        partition_stats_desc = self.driver.scheme_client.describe_path('/Root/.sys/partition_stats')
        partition_stats_permissions = self.permissions_to_dict(partition_stats_desc.permissions)
        self.sort_permissions(partition_stats_permissions)
        result, debug_compare_string = self.compare_permissions_dicts(permissions, partition_stats_permissions)
        assert result, debug_compare_string

    def test_persistence(self):
        permissions = {
            'user': ('ydb.generic.list',)
        }

        self.set_permissions(permissions)
        self.change_cluster_version()
        self.check_permissions(permissions)
