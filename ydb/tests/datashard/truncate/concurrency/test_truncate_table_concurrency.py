# -*- coding: utf-8 -*-
import ydb
import random
import time
import concurrent.futures
from ydb.tests.sql.lib.test_base import TestBase


class TestTruncateTableConcurrency(TestBase):

    left_id_range = 0
    right_id_range = 0

    @classmethod
    def get_extra_feature_flags(cls):
        return [
            "enable_truncate_table",
            "enable_fulltext_index",
        ]

    def test_truncate_with_concurrent_rw_operations(self):
        table_name = f"{self.table_path}_truncate_concurrency"

        self._create_test_table(table_name)

        rows_init_count = 40
        self._upsert_data(table_name, rows_init_count)
        self._verify_table_count(table_name, rows_init_count)

        self._create_secondary_index(table_name)
        self._create_vector_index(table_name)
        self._create_fulltext_index(table_name)

        self._verify_table_count(table_name, rows_init_count)

        self._truncate_table(table_name)
        self._verify_table_count(table_name, 0)

        self._upsert_data(table_name, rows_init_count)
        self._verify_table_count(table_name, rows_init_count)

        operations = [self._select_with_secondary_index, self._select_with_fulltext_index, self._select_with_vector_index, self._upsert_data]
        self.execute_concurrent_operations(table_name, operations)

    def _create_test_table(self, table_name : str):
        self.query(
            f"""
            CREATE TABLE `{table_name}` (
                id Int64 NOT NULL,
                numeric_value_1 Int64 NOT NULL,
                numeric_value_2 Double,
                vector_data String NOT NULL,
                text_data Utf8 NOT NULL,
                PRIMARY KEY (id)
            )
            """
        )

    def _upsert_data(self, table_name : str, rows_count : int = 5):
        sql_upsert = f"""
        UPSERT INTO `{table_name}` (id, numeric_value_1, numeric_value_2, vector_data, text_data)
        VALUES
        """

        self.right_id_range += rows_count

        for i in range(self.left_id_range, self.right_id_range):
            numeric_1 = i * 100 + random.randint(1, 99)
            numeric_2 = round(random.uniform(1.0, 1000.0), 2)

            vector_values = [round(random.uniform(0.0, 1.0), 3) for _ in range(3)]
            vector_list = ",".join(f'{val}f' for val in vector_values)
            text_data = f"test_string_{i}_{random.randint(1000, 9999)}"
            sql_upsert += f"({i}, {numeric_1}, {numeric_2}, Untag(Knn::ToBinaryStringFloat([{vector_list}]), \"FloatVector\"), '{text_data}')"

            if i + 1 == self.right_id_range:
                sql_upsert += ";"
            else:
                sql_upsert += ",\n"

        self.query(sql_upsert)

    def _verify_table_count(self, table_name : str, expected_count : int):
        result = self.query(f"SELECT COUNT(*) as row_count FROM `{table_name}`")
        assert len(result) > 0, "Expected count result"

        row_count = result[0]["row_count"]
        assert row_count == expected_count, f"Expected {expected_count} rows, got {row_count}"

    def _create_secondary_index(self, table_name : str):
        self.query(
            f"""
            ALTER TABLE `{table_name}` ADD INDEX idx_numeric_value_1 GLOBAL ON (numeric_value_1);
            """
        )

    def _create_fulltext_index(self, table_name : str):
        self.query(
            f"""
            ALTER TABLE `{table_name}` ADD INDEX idx_text_data GLOBAL USING fulltext_plain
            ON (text_data)
            WITH (
                tokenizer=standard,
                use_filter_lowercase=true
            );
            """
        )

    def _create_vector_index(self, table_name : str):
        self.query(
            f"""
            ALTER TABLE `{table_name}` ADD INDEX idx_vector_data GLOBAL USING vector_kmeans_tree
            ON (vector_data)
            WITH (similarity=cosine,
                  vector_type=Float,
                  vector_dimension=3,
                  levels=2,
                  clusters=10);
            """
        )

    def _truncate_table(self, table_name : str):
        self.query(
            f"""
            TRUNCATE TABLE `{table_name}`;
            """
        )

        self.left_id_range = self.right_id_range

    def _select_simple(self, table_name : str):
        index = 0

        if self.left_id_range < self.right_id_range:
            index = random.randint(self.left_id_range, self.right_id_range - 1)

        self.query(
            f"""
            SELECT * FROM `{table_name}`
            WHERE id = {index};
            """
        )

    def _select_with_secondary_index(self, table_name : str):
        index = 0

        if self.left_id_range < self.right_id_range:
            index = random.randint(self.left_id_range, self.right_id_range - 1)

        self.query(
            f"""
            SELECT * FROM `{table_name}`
            WHERE numeric_value_1 < {index * 100};
            """
        )

    def _select_with_fulltext_index(self, table_name : str):
        index = 0

        if self.left_id_range < self.right_id_range:
            index = random.randint(self.left_id_range, self.right_id_range - 1)

        self.query(
            f"""
            SELECT `id`, `text_data`
            FROM `{table_name}` VIEW `idx_text_data`
            WHERE FulltextMatch(`text_data`, "string_{index}")
            ORDER BY `id`;
            """
        )

    def _select_with_vector_index(self, table_name : str):
        self.query(
            f"""
            SELECT id, numeric_value_1, numeric_value_2, text_data,
                   Knn::CosineSimilarity(vector_data, Knn::ToBinaryStringFloat([0.1f, 0.2f, 0.3f])) as similarity
            FROM `{table_name}`
            VIEW `idx_vector_data`
            ORDER BY Knn::CosineSimilarity(vector_data, Knn::ToBinaryStringFloat([0.1f, 0.2f, 0.3f])) DESC
            LIMIT 5;
            """
        )

    def execute_concurrent_operations(self, table_name: str, operations: list):
        execution_time = 25
        deadline = time.time() + execution_time

        def random_operations_worker():
            while time.time() < deadline:
                try:
                    operation = random.choice(operations)
                    operation(table_name)
                except ydb.issues.Aborted:
                    continue

        def truncate_worker():
            # We can’t call TRUNCATE TABLE too often for two reasons:
            # 1. TRUNCATE calls can overwhelm the session pool.
            # 2. DML operations may get a TLI too frequently due to an incorrect schemaVersion on the data shard.
            while time.time() < deadline:
                self._truncate_table(table_name)
                time.sleep(2)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            random_future = executor.submit(random_operations_worker)
            truncate_future = executor.submit(truncate_worker)

            concurrent.futures.wait([random_future, truncate_future], timeout=execution_time+5)

            random_future.result(timeout=1)
            truncate_future.result(timeout=1)
