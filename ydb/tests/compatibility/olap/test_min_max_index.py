import time

import pytest

from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestMinMaxIndex(RollingUpgradeAndDowngradeFixture):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (26, 2):
            pytest.skip("min_max index is available starting from 26.2")

        self.index_min_max_name = "idx_min_max"
        self.rows_count = 20
        extra_flags = {}
        extra_flags["enable_local_min_max_index"] = True

        yield from self.setup_cluster(
            extra_feature_flags=extra_flags,
            column_shard_config={
                "disabled_on_scheme_shard": False,
                "alter_object_enabled": True,
            },
        )

    def _table_path(self, table_name):
        return f"{self.database_path}/{table_name}".replace("//", "/")

    def create_table(self, table_name):
        query = f"""
            CREATE TABLE `{table_name}` (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                level Int32,
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1)
            """

        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(query)

    def _add_min_max_index(self, table_name, column, index_name=None):
        index_name = index_name or f"{self.index_min_max_name}_{column}"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                ALTER TABLE `{table_name}`
                ADD INDEX `{index_name}` LOCAL USING min_max
                    ON ({column});
                """
            )

    def _write_data(self, table_name):
        values = []
        for i in range(self.rows_count):
            ts_str = f"2024-01-01T00:00:{i:02d}.000000Z"
            resource_id = f"res_{i % 5}"
            level = i
            uid = f"uid_{i}"
            values.append(f'(Timestamp("{ts_str}"), "{resource_id}", {level}, "{uid}")')

        query = f"""
            INSERT INTO `{table_name}` (timestamp, resource_id, level, uid)
            VALUES {",".join(values)};
            """

        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(query)

    def _get_queries(self, table_name):
        queries = []

        queries.append([
            None,
            """
            UPSERT INTO `{table}` (timestamp, resource_id, level, uid)
            VALUES (Timestamp("2024-06-01T12:00:00.000000Z"), "res_new", 100, "uid_new");
            """.format(table=table_name),
        ])

        queries.append([
            None,
            """
            UPDATE `{table}` SET level = 200
            WHERE timestamp = Timestamp("2024-06-01T12:00:00.000000Z") AND uid = "uid_new";
            """.format(table=table_name),
        ])

        queries.append([
            None,
            """
            UPSERT INTO `{table}` (timestamp, resource_id, level, uid)
            VALUES (Timestamp("2024-06-01T12:00:01.000000Z"), "res_upsert", 300, "uid_upsert");
            """.format(table=table_name),
        ])

        queries.append([
            None,
            'DELETE FROM `{table}` WHERE timestamp = Timestamp("2024-06-01T12:00:01.000000Z") AND uid = "uid_upsert";'.format(table=table_name),
        ])

        queries.append([
            11,
            f"SELECT COUNT(*) AS cnt FROM `{table_name}` WHERE level >= 5 AND level <= 15;",
        ])

        queries.append([
            3,
            f"SELECT COUNT(*) AS cnt FROM `{table_name}` WHERE level > 17;",
        ])

        queries.append([
            12,
            f'SELECT COUNT(*) AS cnt FROM `{table_name}` WHERE resource_id >= "res_2" AND resource_id <= "res_4";',
        ])

        queries.append([
            6,
            f'SELECT COUNT(*) AS cnt FROM `{table_name}` '
            f'WHERE timestamp BETWEEN Timestamp("2024-01-01T00:00:05.000000Z") AND Timestamp("2024-01-01T00:00:10.000000Z");',
        ])

        return queries

    def _request_compaction_and_wait(self, table_name, wait_seconds=15):
        path = self._table_path(table_name)
        stmt = f"ALTER OBJECT `{path}` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);"

        try:
            with ydb.SessionPool(self.driver, size=1) as pool:
                with pool.checkout() as session:
                    session.execute_scheme(stmt)
        except Exception as exc:
            pytest.fail(
                f"Failed to request scheme actualization/compaction for table `{table_name}` "
                f"with statement `{stmt}`: {exc}"
            )
        time.sleep(wait_seconds)

    def _do_queries(self, queries):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for select_result_or_none, query in queries:
                result_sets = session_pool.execute_with_retries(query)
                if select_result_or_none is not None:
                    assert len(result_sets[0].rows) == 1, f"Query '{query}' returned {len(result_sets[0].rows)} rows instead of 1"
                    assert result_sets[0].rows[0]["cnt"] == select_result_or_none, f"query text: '{query}'"

    def test_min_max_index(self):
        table_numeric = "olap_min_max_numeric"
        table_string = "olap_min_max_string"
        table_multi = "olap_min_max_multi"

        self.create_table(table_numeric)
        self._add_min_max_index(table_numeric, column="level")
        self._write_data(table_numeric)

        self.create_table(table_string)
        self._add_min_max_index(table_string, column="resource_id")
        self._write_data(table_string)

        self.create_table(table_multi)
        self._add_min_max_index(table_multi, column="level")
        self._add_min_max_index(table_multi, column="resource_id")
        self._write_data(table_multi)

        for table in (table_numeric, table_string, table_multi):
            self._request_compaction_and_wait(table)

        for _ in self.roll():
            self._do_queries(self._get_queries(table_numeric))
            self._do_queries(self._get_queries(table_string))
            self._do_queries(self._get_queries(table_multi))
