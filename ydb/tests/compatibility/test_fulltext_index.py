import pytest
import re
import ydb as ydbs

from ydb.tests.library.fixtures import fulltext
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb


class TestFulltextIndex(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (26, 1):
            pytest.skip("Only available since 26-1")
        self.row_count = 50
        self.query_count = 5
        self.limit = 5
        self.good_queries = {}
        yield from self.setup_cluster(extra_feature_flags={"enable_fulltext_index": True})

    def create_table(self, table_name):
        query = f"""
            CREATE TABLE {table_name} (
                key Int64 NOT NULL,
                text String NOT NULL,
                PRIMARY KEY (key)
            )
            """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(query)

    def _write_data(self, table_name):
        good = []
        values = []
        for key in range(self.row_count):
            text = fulltext.get_random_text()
            values.append(f'({key}, "{text}")')
            words = text.split(' ')
            good_query = []
            for w in words:
                if len(w) >= 4 and re.fullmatch('\\w+', w):
                    good_query.append(w)
                    if len(good_query) >= 3:
                        break
            if len(good_query) >= 3:
                good.append(' '.join(good_query))
        self.good_queries[table_name] = good
        sql_upsert = f"""
            UPSERT INTO `{table_name}` (`key`, `text`)
            VALUES {",".join(values)};
            """
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(sql_upsert)

    def _create_index(self, table_name, index_name, index_type, tokenizer='standard'):
        create_index_sql = f"""
            ALTER TABLE `{table_name}`
            ADD INDEX `{index_name}` GLOBAL USING {index_type}
            ON (text)
            WITH (
                tokenizer={tokenizer},
                use_filter_lowercase=true,
                use_filter_snowball=true,
                language="english"
            );
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
        for text_type in ['string', 'utf8']:
            for index_type in ['fulltext_plain', 'fulltext_relevance']:
                for tokenizer in ['standard', 'whitespace']:
                    queries.extend(self._get_queries_for(text_type, index_type, tokenizer))
        return queries

    def _get_queries_for(self, text_type, index_type, tokenizer):
        table_name = f"table_{text_type}"
        index_name = f"idx_{index_type}_{tokenizer}"
        queries = []
        for i in range(self.query_count):
            query = ' '.join(self.good_queries[table_name][i])
            queries.append([
                True, f"""
                SELECT `pk`, `text`
                FROM `{table_name}`
                VIEW `{index_name}`
                WHERE FulltextMatch(`text`, "{query}")
                LIMIT {self.limit};
                """
            ])
            if index_type == 'fulltext_relevance':
                queries.append([
                    True, f"""
                    SELECT `pk`, `text`, FulltextScore(`text`, "{query}") as `rel`
                    FROM `{table_name}`
                    VIEW `{index_name}`
                    ORDER BY `rel`
                    LIMIT {self.limit};
                    """
                ])
            # Insert, update, upsert, delete
            key = self.row_count+1
            queries.append([
                False, f"""
                INSERT INTO `{table_name}` (`key`, `text`)
                VALUES ({key}, "{fulltext.get_random_text()}")
                """
            ])
            queries.append([
                False, f"""
                UPDATE `{table_name}` SET `text`="{fulltext.get_random_text()}"
                WHERE key={key}
                """
            ])
            queries.append([
                False, f"""
                UPSERT INTO `{table_name}` (`key`, `text`)
                VALUES ({key}, "{fulltext.get_random_text()}")
                """
            ])
            queries.append([
                False, f"""
                DELETE FROM `{table_name}` WHERE key={key}
                """
            ])
        return queries

    def _do_queries(self, queries):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            for [is_select, query] in queries:
                result_sets = session_pool.execute_with_retries(query)
                if is_select:
                    assert len(result_sets[0].rows) > 0, "Query returned an empty set"
                    rows = result_sets[0].rows
                    for row in rows:
                        assert 'rel' not in row or row['rel'] is not None, "relevance is None"

    def select_from_index(self):
        queries = self._get_queries()
        for _ in self.roll():
            self._do_queries(queries)

    def select_from_index_without_roll(self):
        queries = self._get_queries()
        self._do_queries(queries)

    def test_fulltext_index(self):
        for text_type in ['string', 'utf8']:
            table_name = f"table_{text_type}"
            self.create_table(table_name)
            self._write_data(table_name)
            for index_type in ['fulltext_plain', 'fulltext_relevance']:
                for tokenizer in ['standard', 'whitespace']:
                    index_name = f"idx_{index_type}_{tokenizer}"
                    self._create_index(
                        table_name=table_name,
                        index_name=index_name,
                        index_type=index_type,
                        tokenizer=tokenizer,
                    )
        self.wait_index_ready()
        self.select_from_index()
