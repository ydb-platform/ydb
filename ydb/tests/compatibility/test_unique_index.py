import pytest
import re
import ydb as ydbs
import random

from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestUniqueIndex(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (25, 4):
            pytest.skip("Only available since 25-4")
        flags = {"enable_add_unique_index": True}
        if min(self.versions) < (26, 2):
            self.online_unique = False
        else:
            self.online_unique = True
            flags["enable_online_add_unique_index"] = True
        self.row_count = 50
        self.append_row_count = 5
        yield from self.setup_cluster(extra_feature_flags=flags)

    def create_table(self, table_name, with_index):
        if with_index:
            with_index = ", INDEX idx_uniq GLOBAL UNIQUE ON (uniq)"
        else:
            with_index = ""
        query = f"""
            CREATE TABLE {table_name} (
                key Uint64 NOT NULL,
                uniq String NOT NULL,
                PRIMARY KEY (key)
                {with_index}
            )
            """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(query)

    def _write_initial(self, table_name):
        values = []
        self.cur_key = 1
        while self.cur_key <= self.row_count:
            values.append(f'({self.cur_key}, "{self.cur_key}")')
            self.cur_key = self.cur_key+1
        return f"""
            UPSERT INTO `{table_name}` (`key`, `uniq`)
            VALUES {",".join(values)};
            """

    def _get_queries(self, table_name, with_index):
        queries = []
        queries.append([ "ok", self._write_initial(table_name) ])
        if not with_index:
            queries.append([
                "ok", f"""
                    ALTER TABLE `{table_name}`
                    ADD INDEX idx_uniq GLOBAL UNIQUE ON (uniq)
                """
            ])
        while self.cur_key <= self.row_count + self.append_row_count:
            queries.append([
                "select", f"""
                SELECT `key`, `uniq` FROM `{table_name}` VIEW idx_uniq
                WHERE `key` IN ({self.row_count}, {self.row_count-1}, {self.row_count-2})
                """
            ])
            queries.append([
                "ok", f"""
                INSERT INTO `{table_name}` (`key`, `uniq`)
                VALUES ({self.cur_key}, "{self.cur_key}")
                """
            ])
            queries.append([
                "ok", f"""
                UPSERT INTO `{table_name}` (`key`, `uniq`)
                VALUES ({self.cur_key+1}, "{self.cur_key+1}")
                """
            ])
            queries.append([
                "ok", f"""
                UPDATE `{table_name}` SET `uniq`="{self.cur_key+2}" WHERE `key`={self.cur_key+1}
                """
            ])
            queries.append([
                "ok", f"""
                    ALTER TABLE `{table_name}` DROP INDEX idx_uniq
                """
            ])
            queries.append([
                "ok", f"""
                    ALTER TABLE `{table_name}`
                    ADD INDEX idx_uniq GLOBAL UNIQUE ON (uniq)
                """
            ])
            queries.append([
                "ok", f"""
                DELETE FROM `{table_name}` WHERE `key`={self.cur_key+1}
                """
            ])
            queries.append([
                "error", f"""
                INSERT INTO `{table_name}` (`key`, `uniq`)
                VALUES ({self.cur_key+1}, "{random.randint(1, self.row_count)}")
                """
            ])
            queries.append([
                "error", f"""
                INSERT INTO `{table_name}` (`key`, `uniq`)
                VALUES ({self.cur_key+1}, "{self.cur_key}")
                """
            ])
            queries.append([
                "error", f"""
                UPSERT INTO `{table_name}` (`key`, `uniq`)
                VALUES ({self.cur_key+1}, "{self.cur_key}")
                """
            ])
            queries.append([
                "error", f"""
                UPDATE `{table_name}` SET `uniq`="1"
                WHERE `key`={random.randint(2, self.row_count)}
                """
            ])
            self.cur_key = self.cur_key+1
        if not with_index:
            queries.append([
                "ok", f"ALTER TABLE `{table_name}` DROP INDEX idx_uniq"
            ])
        queries.append([
            "ok", f"DELETE FROM `{table_name}`"
        ])
        return queries

    def _do_queries(self, table_name, with_index):
        queries = self._get_queries(table_name, with_index)
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for [query_type, query] in queries:
                if query_type == "error":
                    ok = False
                    try:
                        session_pool.execute_with_retries(query)
                    except ydbs.issues.PreconditionFailed as ex:
                        ok = ("Conflict with existing key" in str(ex))
                        pass
                    assert ok, "Conflict expected"
                elif query_type == "select":
                    result_sets = session_pool.execute_with_retries(query)
                    assert len(result_sets[0].rows) > 0, "Query returned an empty set"
                else:
                    session_pool.execute_with_retries(query)

    def test_unique_index(self):
        self.create_table("test_table", False)
        self.create_table("test_indexed", True)
        for _ in self.roll():
            self._do_queries("test_table", False)
            self._do_queries("test_indexed", True)
