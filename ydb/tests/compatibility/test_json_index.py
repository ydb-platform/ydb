import pytest
import ydb as ydbs

from ydb.tests.library.fixtures import json
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestJsonIndex(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (26, 3):
            pytest.skip("Only available since 26-3")
        self.row_count = 50
        self.query_count = 5
        self.limit = 5
        yield from self.setup_cluster(extra_feature_flags={"enable_json_index": True})

    def create_table(self, table_name, json_type):
        query = f"""
            CREATE TABLE {table_name} (
                pk Uint64 NOT NULL,
                json_data {json_type} NOT NULL,
                PRIMARY KEY (pk)
            )
        """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(query)

    def _write_data(self, table_name, json_type):
        values = []
        for key in range(self.row_count):
            json_value = json.get_random_json()
            values.append(f'({key}, {json_type}(\'{json_value}\'))')

        sql_upsert = f"""
            UPSERT INTO `{table_name}` (`pk`, `json_data`)
            VALUES {",".join(values)};
        """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(sql_upsert)

    def _create_index(self, table_name, index_name):
        create_index_sql = f"""
            ALTER TABLE `{table_name}`
            ADD INDEX `{index_name}` GLOBAL USING json
            ON (json_data);
        """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(create_index_sql)

    def wait_index_ready(self):
        def predicate():
            try:
                self.select_from_index_without_roll()
            except ydbs.issues.SchemeError as ex:
                if "Required global index not found, index name" in str(ex):
                    return False
                raise ex
            return True

        assert wait_for(predicate, timeout_seconds=100, step_seconds=1), "Error getting index status"

    def _get_queries(self):
        queries = []
        for json_type in ['Json', 'JsonDocument']:
            table_name = f"table_{json_type.lower()}"
            index_name = f"idx_{json_type.lower()}"
            queries.extend(self._get_queries_for(table_name, index_name, json_type))
        return queries

    def _get_queries_for(self, table_name, index_name, json_type):
        queries = []
        for _ in range(self.query_count):
            predicate = json.get_random_predicate()
            queries.append(f"""
                SELECT `pk`, `json_data`
                FROM `{table_name}`
                VIEW `{index_name}`
                WHERE {predicate}
                LIMIT {self.limit};
            """)

            key = self.row_count + 1

            queries.append(f"""
                INSERT INTO `{table_name}` (`pk`, `json_data`)
                VALUES ({key}, {json_type}('{json.get_random_json()}'))
            """)

            queries.append(f"""
                UPDATE `{table_name}` SET `json_data` = {json_type}('{json.get_random_json()}')
                WHERE pk = {key}
            """)

            queries.append(f"""
                UPSERT INTO `{table_name}` (`pk`, `json_data`)
                VALUES ({key}, {json_type}('{json.get_random_json()}'))
            """)

            queries.append(f"""
                DELETE FROM `{table_name}` WHERE pk = {key}
            """)
        return queries

    def _do_queries(self, queries):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for query in queries:
                session_pool.execute_with_retries(query)

    def select_from_index(self):
        queries = self._get_queries()
        for _ in self.roll():
            self._do_queries(queries)

    def select_from_index_without_roll(self):
        queries = self._get_queries()
        self._do_queries(queries)

    def test_json_index(self):
        for json_type in ['Json', 'JsonDocument']:
            table_name = f"table_{json_type.lower()}"
            self.create_table(table_name, json_type)
            self._write_data(table_name, json_type)

            index_name = f"idx_{json_type.lower()}"
            self._create_index(
                table_name=table_name,
                index_name=index_name,
            )

        self.wait_index_ready()
        self.select_from_index()
