import pytest

from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb
from enum import Enum


class TestEncoding(RollingUpgradeAndDowngradeFixture):
    class QueryType(Enum):
        SELECT_CNT = 1
        SELECT_ROWS = 2
        MODIFY_DATA = 3

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        self.skip_if_unsupported()

        self.table_name = "encoding_table"
        self.rows_count = 640

        yield from self.setup_cluster(
            extra_feature_flags={"enable_cs_dictionary_encoding": True},
            column_shard_config={
                "disabled_on_scheme_shard": False,
                "alter_object_enabled": True,
            },
        )

    def _create_table(self):
        query = f"""
            CREATE TABLE `{self.table_name}` (
                timestamp Timestamp NOT NULL ENCODING(),
                resource_id Utf8 ENCODING(DICT),
                uid Utf8 NOT NULL ENCODING(OFF),
                PRIMARY KEY (timestamp, uid)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 64)
            """

        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(query)

    def _write_data(self, step):
        values = []
        for i in range(self.rows_count):
            ts_str = f"2024-01-01T00:00:00.{i:06d}Z"
            resource_id = f"res_{i % 5}"
            uid = f"uid_{i}_{step}"
            values.append(f'(Timestamp("{ts_str}"), "{resource_id}", "{uid}")')

        query = f"""
            INSERT INTO `{self.table_name}` (timestamp, resource_id, uid)
            VALUES {",".join(values)};
            """

        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(query)

    def _get_queries(self):
        queries = []

        queries.append([
            self.QueryType.SELECT_CNT,
            f"SELECT COUNT(*) AS cnt FROM `{self.table_name}` WHERE resource_id = \"res_1\";",
        ])

        queries.append([
            self.QueryType.SELECT_CNT,
            f"SELECT COUNT(*) AS cnt FROM `{self.table_name}` WHERE uid = \"uid_0_0\";",
        ])

        queries.append([
            self.QueryType.SELECT_ROWS,
            """
            PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";
            SELECT SOME(resource_id) FROM `{table}` GROUP BY resource_id;
            """.format(table=self.table_name),
        ])

        queries.append([
            self.QueryType.MODIFY_DATA,
            """
            UPSERT INTO `{table}` (timestamp, resource_id, uid)
            VALUES (Timestamp("2024-06-01T12:00:00.000000Z"), "res_new", "uid_new");
            """.format(table=self.table_name),
        ])

        queries.append([
            self.QueryType.MODIFY_DATA,
            """
            UPDATE `{table}` SET resource_id = "res_updated"
            WHERE timestamp = Timestamp("2024-06-01T12:00:00.000000Z") AND uid = "uid_new";
            """.format(table=self.table_name),
        ])

        queries.append([
            self.QueryType.MODIFY_DATA,
            """
            UPSERT INTO `{table}` (timestamp, resource_id, uid)
            VALUES (Timestamp("2024-06-01T12:00:01.000000Z"), "res_upsert", "uid_upsert");
            """.format(table=self.table_name),
        ])

        queries.append([
            self.QueryType.MODIFY_DATA,
            'DELETE FROM `{table}` WHERE timestamp = Timestamp("2024-06-01T12:00:01.000000Z") AND uid = "uid_upsert";'.format(table=self.table_name),
        ])

        return queries

    def _do_queries(self, queries):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for query_type, query in queries:
                result_sets = session_pool.execute_with_retries(query)
                if query_type == self.QueryType.SELECT_CNT:
                    assert len(result_sets[0].rows) > 0, "Query returned no rows"
                    for row in result_sets[0].rows:
                        assert row["cnt"] is not None
                elif query_type == self.QueryType.SELECT_ROWS:
                    assert len(result_sets[0].rows) > 0, "Query returned no rows"

    def skip_if_unsupported(self):
        if min(self.versions) < (26, 3):
            pytest.skip("Only available since stable-26-3")

    def test_encoding(self):
        self.skip_if_unsupported()

        step = 0
        self._create_table()
        self._write_data(step)
        queries = self._get_queries()
        self._do_queries(queries)

        for _ in self.roll():
            step += 1
            self._write_data(step)
            self._do_queries(queries)
