# -*- coding: utf-8 -*-
import pytest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR

import ydb


TEXT_TYPES = ("String", "Utf8")


class TestFulltextAndHybridDocumentationAudit:
    @classmethod
    def setup_class(cls):
        config = KikimrConfigGenerator(
            erasure=Erasure.NONE,
            extra_feature_flags=[
                "enable_fulltext_index",
            ],
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database="/Root",
                endpoint=cls.cluster.nodes[1].endpoint,
            )
        )
        cls.driver.wait(timeout=60)

    @classmethod
    def teardown_class(cls):
        cls.driver.stop()
        cls.cluster.stop()

    def execute(self, query):
        with ydb.QuerySessionPool(self.driver) as pool:
            return pool.execute_with_retries(query)

    @staticmethod
    def text_literal(value, text_type):
        suffix = "u" if text_type == "Utf8" else ""
        return f'"{value}"{suffix}'

    def create_fulltext_table(self, name, text_type, with_ngram=False):
        ngram_options = """
            use_filter_ngram=true,
            filter_ngram_min_length=2,
            filter_ngram_max_length=5,
        """ if with_ngram else ""
        self.execute(
            f"""
                CREATE TABLE `{name}` (
                    id Uint64 NOT NULL,
                    body {text_type},
                    PRIMARY KEY (id),
                    INDEX ft_idx GLOBAL USING fulltext_plain ON (body)
                        WITH (
                            tokenizer=standard,
                            {ngram_options}
                            use_filter_lowercase=true
                        )
                );
            """
        )

    @pytest.mark.parametrize("text_type", TEXT_TYPES)
    @pytest.mark.parametrize("operation", ("insert", "update", "delete"))
    def test_fulltext_index_observes_writes_in_same_transaction(self, text_type, operation):
        table = f"fulltext_ryw_{operation}_{text_type.lower()}"
        self.create_fulltext_table(table, text_type)

        old_value = "deleted" if operation == "delete" else "old"
        if operation != "insert":
            self.execute(
                f"""
                    UPSERT INTO `{table}` (id, body) VALUES
                        (1, {self.text_literal(old_value, text_type)});
                """
            )

        if operation == "insert":
            mutation = (
                f"UPSERT INTO `{table}` (id, body) VALUES "
                f"(1, {self.text_literal('inserted', text_type)});"
            )
            searched_value = "inserted"
            expected = [1]
        elif operation == "update":
            mutation = (
                f"UPDATE `{table}` SET body = "
                f"{self.text_literal('new', text_type)} WHERE id = 1;"
            )
            searched_value = "new"
            expected = [1]
        else:
            mutation = f"DELETE FROM `{table}` WHERE id = 1;"
            searched_value = "deleted"
            expected = []

        result_sets = self.execute(
            f"""
                {mutation}

                SELECT id FROM `{table}` VIEW ft_idx
                WHERE FulltextMatch(body, "{searched_value}");
            """
        )

        assert [row.id for row in result_sets[0].rows] == expected

    @pytest.mark.parametrize("text_type", TEXT_TYPES)
    def test_multisegment_like_accepts_literal_of_column_type(self, text_type):
        table = f"fulltext_like_{text_type.lower()}"
        self.create_fulltext_table(table, text_type, with_ngram=True)
        self.execute(
            f"""
                UPSERT INTO `{table}` (id, body) VALUES
                    (1, {self.text_literal('обучение', text_type)}),
                    (2, {self.text_literal('переобучение', text_type)}),
                    (3, {self.text_literal('машина', text_type)});
            """
        )

        pattern = self.text_literal("%обуч%ние%", text_type)
        result_sets = self.execute(
            f"""
                SELECT id FROM `{table}` VIEW ft_idx
                WHERE body LIKE {pattern}
                ORDER BY id;
            """
        )

        assert [row.id for row in result_sets[0].rows] == [1, 2]

    @pytest.mark.parametrize("text_type", TEXT_TYPES)
    def test_query_mode_excludes_terms_prefixed_with_minus(self, text_type):
        table = f"fulltext_query_mode_{text_type.lower()}"
        self.create_fulltext_table(table, text_type)
        self.execute(
            f"""
                UPSERT INTO `{table}` (id, body) VALUES
                    (1, {self.text_literal('machine learning', text_type)}),
                    (2, {self.text_literal('machine databases', text_type)}),
                    (3, {self.text_literal('databases only', text_type)});
            """
        )

        result_sets = self.execute(
            f"""
                SELECT id FROM `{table}` VIEW ft_idx
                WHERE FulltextMatch(body, "+machine -databases", "Query" AS Mode)
                ORDER BY id;
            """
        )

        assert [row.id for row in result_sets[0].rows] == [1]

    def test_scripting_api_hybrid_linear_mode_returns_ranked_rows(self):
        self.execute(
            """
                CREATE TABLE `hybrid_linear` (
                    id Uint64 NOT NULL,
                    text Utf8,
                    embedding String,
                    PRIMARY KEY (id)
                );
            """
        )
        self.execute(
            """
                UPSERT INTO `hybrid_linear` (id, text, embedding) VALUES
                    (1, "cats cats cats love"u,
                        Untag(Knn::ToBinaryStringUint8(
                            Cast([240, 15] AS List<Uint8>)), "Uint8Vector")),
                    (2, "dogs and foxes run"u,
                        Untag(Knn::ToBinaryStringUint8(
                            Cast([250, 10] AS List<Uint8>)), "Uint8Vector")),
                    (3, "cats sleep"u,
                        Untag(Knn::ToBinaryStringUint8(
                            Cast([10, 250] AS List<Uint8>)), "Uint8Vector")),
                    (4, "birds fly high"u,
                        Untag(Knn::ToBinaryStringUint8(
                            Cast([200, 60] AS List<Uint8>)), "Uint8Vector"));
            """
        )
        self.execute(
            """
                ALTER TABLE `hybrid_linear` ADD INDEX ft_idx
                    GLOBAL USING fulltext_relevance ON (text)
                    WITH (tokenizer=standard, use_filter_lowercase=true);
            """
        )
        self.execute(
            """
                ALTER TABLE `hybrid_linear` ADD INDEX vec_idx
                    GLOBAL USING vector_kmeans_tree ON (embedding)
                    WITH (
                        distance=cosine,
                        vector_type="uint8",
                        vector_dimension=2,
                        levels=2,
                        clusters=2
                    );
            """
        )

        result = ydb.ScriptingClient(self.driver).execute_yql(
            """
                PRAGMA ydb.KMeansTreeSearchTopSize = "4";
                $target = Untag(Knn::ToBinaryStringUint8(
                    Cast([250, 10] AS List<Uint8>)), "Uint8Vector");

                SELECT id FROM `hybrid_linear`
                ORDER BY HybridRank(
                    FulltextScore(text, "cats"),
                    Knn::CosineDistance(embedding, $target),
                    "linear" AS Mode)
                LIMIT 4;
            """
        )

        assert {row.id for row in result.result_sets[0].rows} == {1, 2, 3, 4}
