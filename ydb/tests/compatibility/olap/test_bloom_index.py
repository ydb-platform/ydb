import time

import pytest

from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestBloomIndex(RollingUpgradeAndDowngradeFixture):

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        self.index_bloom_name = "idx_bloom"
        self.index_ngram_name = "idx_ngram"
        self.rows_count = 20
        self._use_sql_index_syntax = min(self.versions) >= (26, 1)

        extra_flags = {}
        if self._use_sql_index_syntax:
            extra_flags["enable_local_bloom_filter_index"] = True
            extra_flags["enable_local_bloom_ngram_filter_index"] = True

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
                uid Utf8 NOT NULL,
                PRIMARY KEY (timestamp, uid)
            )
            PARTITION BY HASH(timestamp, uid)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1)
            """

        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(query)

    def _add_bloom_index(self, table_name, column="resource_id"):
        if self._use_sql_index_syntax:
            with ydb.QuerySessionPool(self.driver) as session_pool:
                session_pool.execute_with_retries(
                    f"""
                    ALTER TABLE `{table_name}`
                    ADD INDEX `{self.index_bloom_name}` LOCAL USING bloom_filter
                        ON ({column})
                        WITH (false_positive_probability = 0.01);
                    """
                )
        else:
            path = self._table_path(table_name)
            stmt = (
                f'ALTER OBJECT `{path}` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME={self.index_bloom_name}, '
                f'TYPE=BLOOM_FILTER, FEATURES=`{{"column_name": "{column}", "false_positive_probability": 0.01}}`);'
            )

            with ydb.SessionPool(self.driver, size=1) as pool:
                with pool.checkout() as session:
                    session.execute_scheme(stmt)

    def _add_ngram_index(self, table_name, column="resource_id"):
        if self._use_sql_index_syntax:
            with ydb.QuerySessionPool(self.driver) as session_pool:
                session_pool.execute_with_retries(
                    f"""
                    ALTER TABLE `{table_name}`
                    ADD INDEX `{self.index_ngram_name}` LOCAL USING bloom_ngram_filter
                        ON ({column})
                        WITH (ngram_size = 3, false_positive_probability = 0.01, case_sensitive = true);
                    """
                )
        else:
            path = self._table_path(table_name)
            stmt = (
                f'ALTER OBJECT `{path}` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME={self.index_ngram_name}, '
                f'TYPE=BLOOM_NGRAMM_FILTER, FEATURES=`{{"column_name": "{column}", "ngramm_size": 3, '
                f'"hashes_count": 2, "filter_size_bytes": 4096, "records_count": 50000, '
                f'"case_sensitive": true}}`);'
            )

            with ydb.SessionPool(self.driver, size=1) as pool:
                with pool.checkout() as session:
                    session.execute_scheme(stmt)

    def _write_data(self, table_name):
        values = []
        for i in range(self.rows_count):
            ts_str = f"2024-01-01T00:00:{i:02d}.000000Z"
            resource_id = f"res_{i % 5}"
            uid = f"uid_{i}"
            values.append(f'(Timestamp("{ts_str}"), "{resource_id}", "{uid}")')

        query = f"""
            INSERT INTO `{table_name}` (timestamp, resource_id, uid)
            VALUES {",".join(values)};
            """

        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(query)

    def _get_queries(self, table_name):
        queries = []

        queries.append([
            True,
            f"SELECT COUNT(*) AS cnt FROM `{table_name}` WHERE resource_id = \"res_1\";",
        ])

        queries.append([
            True,
            f"SELECT COUNT(*) AS cnt FROM `{table_name}` WHERE uid = \"uid_0\";",
        ])

        queries.append([
            False,
            """
            UPSERT INTO `{table}` (timestamp, resource_id, uid)
            VALUES (Timestamp("2024-06-01T12:00:00.000000Z"), "res_new", "uid_new");
            """.format(table=table_name),
        ])

        queries.append([
            False,
            """
            UPDATE `{table}` SET resource_id = "res_updated"
            WHERE timestamp = Timestamp("2024-06-01T12:00:00.000000Z") AND uid = "uid_new";
            """.format(table=table_name),
        ])

        queries.append([
            False,
            """
            UPSERT INTO `{table}` (timestamp, resource_id, uid)
            VALUES (Timestamp("2024-06-01T12:00:01.000000Z"), "res_upsert", "uid_upsert");
            """.format(table=table_name),
        ])

        queries.append([
            False,
            'DELETE FROM `{table}` WHERE timestamp = Timestamp("2024-06-01T12:00:01.000000Z") AND uid = "uid_upsert";'.format(table=table_name),
        ])

        return queries

    def _request_compaction_and_wait(self, table_name, wait_seconds=15):
        path = self._table_path(table_name)
        stmt = f"ALTER OBJECT `{path}` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);"

        try:
            with ydb.SessionPool(self.driver, size=1) as pool:
                with pool.checkout() as session:
                    session.execute_scheme(stmt)
        except Exception:
            pass

        time.sleep(wait_seconds)

    def _do_queries(self, queries):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for is_select, query in queries:
                result_sets = session_pool.execute_with_retries(query)
                if is_select:
                    assert len(result_sets[0].rows) > 0, "Query returned no rows"
                    for row in result_sets[0].rows:
                        assert row["cnt"] is not None

    def test_bloom_index(self):
        table_bloom = "olap_bloom"
        table_ngram = "olap_ngram"
        table_both = "olap_both"

        self.create_table(table_bloom)
        self._add_bloom_index(table_bloom)
        self._write_data(table_bloom)

        self.create_table(table_ngram)
        self._add_ngram_index(table_ngram)
        self._write_data(table_ngram)

        self.create_table(table_both)
        self._add_bloom_index(table_both, column="uid")
        self._add_ngram_index(table_both, column="resource_id")
        self._write_data(table_both)

        for table in (table_bloom, table_ngram, table_both):
            self._request_compaction_and_wait(table)

        for _ in self.roll():
            self._do_queries(self._get_queries(table_bloom))
            self._do_queries(self._get_queries(table_ngram))
            self._do_queries(self._get_queries(table_both))
