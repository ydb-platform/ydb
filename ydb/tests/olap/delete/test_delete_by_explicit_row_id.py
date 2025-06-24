from .base import DeleteTestBase
from ydb.tests.library.test_meta import link_test_case
import random


class TestDeleteByExplicitRowId(DeleteTestBase):
    test_name = "delete_by_explicit_row_id"
    MAX_ID = 2**32 // 2 - 1

    @classmethod
    def setup_class(cls):
        super(TestDeleteByExplicitRowId, cls).setup_class()

    def _get_test_dir(self):
        return f"{self.ydb_client.database}/{self.test_name}"

    def _get_row_count(self, table_path):
        return self.ydb_client.query(f"SELECT count(*) as Rows from `{table_path}`")[0].rows[0]["Rows"]

    def _test_single_column_pk(self, rows_to_insert, rows_to_delete, iterations):
        table_path = f"{self._get_test_dir()}/testTableSimplePk"
        self.ydb_client.query(f"DROP TABLE IF EXISTS `{table_path}`;")
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Int32 NOT NULL,
                value Int64,
                PRIMARY KEY(id),
            )
            WITH (
                STORE = COLUMN
            )
            """
        )

        all_rows_ids = random.sample(range(self.MAX_ID), rows_to_insert * iterations)
        rows_in_table = 0
        for it in range(0, len(all_rows_ids), rows_to_insert):
            rows_ids = all_rows_ids[it : it + rows_to_insert]

            insert_query = f"INSERT INTO `{table_path}` (id, value) VALUES "
            for i in rows_ids:
                insert_query += f"({i}, {i}), "
            insert_query = insert_query[:-2] + ";"
            self.ydb_client.query(insert_query)

            rows_in_table += rows_to_insert
            assert self._get_row_count(table_path) == rows_in_table

            random.shuffle(rows_ids)
            rows_ids = rows_ids[:rows_to_delete]
            for i in rows_ids:
                self.ydb_client.query(
                    f"""
                        DELETE FROM `{table_path}` WHERE id = {i}
                    """
                )
                rows_in_table -= 1
                assert self._get_row_count(table_path) == rows_in_table
                assert (
                    self.ydb_client.query(f"SELECT count(*) as Rows from `{table_path}` WHERE id = {i}")[0].rows[0][
                        "Rows"
                    ]
                    == 0
                )

    def _test_two_columns_pk(self, rows_to_insert, rows_to_delete, iterations):
        table_path = f"{self._get_test_dir()}/testTableComplexPk"
        self.ydb_client.query(f"DROP TABLE IF EXISTS `{table_path}`;")
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id1 Int32 NOT NULL,
                id2 Utf8 NOT NULL,
                value Int64,
                PRIMARY KEY(id1, id2),
            )
            WITH (
                STORE = COLUMN
            )
            """
        )

        all_rows_ids = random.sample(range(self.MAX_ID), rows_to_insert * iterations)
        rows_in_table = 0
        for it in range(0, len(all_rows_ids), rows_to_insert):
            rows_ids = all_rows_ids[it : it + rows_to_insert]

            insert_query = f"INSERT INTO `{table_path}` (id1, id2, value) VALUES "
            for i in rows_ids:
                insert_query += f"({i}, '{i}', {i}), "
            insert_query = insert_query[:-2] + ";"
            self.ydb_client.query(insert_query)

            rows_in_table += rows_to_insert
            assert self._get_row_count(table_path) == rows_in_table

            random.shuffle(rows_ids)
            rows_ids = rows_ids[:rows_to_delete]
            for i in rows_ids:
                self.ydb_client.query(
                    f"""
                        DELETE FROM `{table_path}` WHERE id1 = {i} AND id2 = '{i}'
                    """
                )
                rows_in_table -= 1
                assert self._get_row_count(table_path) == rows_in_table
                assert (
                    self.ydb_client.query(
                        f"SELECT count(*) as Rows from `{table_path}` WHERE id1 = {i} AND id2 = '{i}'"
                    )[0].rows[0]["Rows"]
                    == 0
                )

    @link_test_case("#13529")
    def test_delete_row_by_explicit_row_id(self):
        self._test_single_column_pk(rows_to_insert=1000, rows_to_delete=10, iterations=10)
        self._test_single_column_pk(rows_to_insert=10, rows_to_delete=10, iterations=10)
        self._test_two_columns_pk(rows_to_insert=1000, rows_to_delete=10, iterations=10)
        self._test_two_columns_pk(rows_to_insert=10, rows_to_delete=10, iterations=10)
