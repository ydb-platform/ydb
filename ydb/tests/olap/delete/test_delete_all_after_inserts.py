from .base import DeleteTestBase
import random


class TestDeleteAllAfterInserts(DeleteTestBase):
    test_name = "delete_by_explicit_row_id"
    MAX_ID = 2**32 // 2 - 1

    @classmethod
    def setup_class(cls):
        super(TestDeleteAllAfterInserts, cls).setup_class()

    def _get_test_dir(self):
        return f"{self.ydb_client.database}/{self.test_name}"

    def _get_row_count(self, table_path):
        return self.ydb_client.query(f"SELECT count(*) as Rows from `{table_path}`")[0].rows[0]["Rows"]

    def _test_delete_all(self, rows_to_insert, insert_iterations):
        table_path = f"{self._get_test_dir()}/testTableAllDeleted"
        self.ydb_client.query(f"DROP TABLE IF EXISTS `{table_path}`;")
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id1 Int32 NOT NULL,
                id2 Int32 NOT NULL,
                value Int64,
                PRIMARY KEY(id1, id2),
            )
            WITH (
                STORE = COLUMN
            )
            """
        )

        all_rows_ids = random.sample(range(self.MAX_ID), rows_to_insert * insert_iterations)
        rows_in_table = 0
        for it in range(0, len(all_rows_ids), rows_to_insert):
            rows_ids = all_rows_ids[it:it + rows_to_insert]

            insert_query = f"INSERT INTO `{table_path}` (id1, id2, value) VALUES "
            for i in rows_ids:
                insert_query += f"({i}, {i}, {i}), "
            insert_query = insert_query[:-2] + ";"
            self.ydb_client.query(insert_query)

            rows_in_table += rows_to_insert
            assert self._get_row_count(table_path) == rows_in_table

        self.ydb_client.query(f"DELETE FROM `{table_path}`")

        assert self._get_row_count(table_path) == 0

        assert len(self.ydb_client.query(
            f"""
            SELECT id1 as id1, id2 as id2 FROM `{table_path}` WHERE id1 != 10000000 ORDER by id1 ASC, id2 ASC LIMIT 100;
            """
        )[0].rows) == 0

        assert len(self.ydb_client.query(
            f"""
            SELECT id1 as id1, id2 as id2 FROM `{table_path}` WHERE id1 != 10000000 ORDER by id1 ASC, id2 ASC;
            """
        )[0].rows) == 0

        assert len(self.ydb_client.query(
            f"""
            SELECT id1 as id1, id2 as id2 FROM `{table_path}` WHERE id1 != 10000000 ORDER by id1 DESC, id2 DESC;
            """
        )[0].rows) == 0

        assert len(self.ydb_client.query(
            f"""
            SELECT id1 as id1, id2 as id2 FROM `{table_path}` WHERE id1 != 10000000 LIMIT 100;
            """
        )[0].rows) == 0

        assert len(self.ydb_client.query(
            f"""
            SELECT id1 as id1, id2 as id2 FROM `{table_path}` WHERE id1 != 10000000;
            """
        )[0].rows) == 0

        assert len(self.ydb_client.query(
            f"""
            SELECT id1 as id1, id2 as id2 FROM `{table_path}` WHERE id1 != 10000000 ORDER by id1 DESC, id2 DESC LIMIT 100;
            """
        )[0].rows) == 0

    def _test_select_query(self, query, reversed=False):
        res = self.ydb_client.query(query)[0].rows
        prev_row = None
        for i in range(len(res)):
            row = res[i]
            assert row["value"] == 'NEW', row["id1"]
            if prev_row is not None:
                if reversed:
                    assert row["id1"] < prev_row["id1"]
                else:
                    assert row["id1"] > prev_row["id1"]
            prev_row = row

    def _test_update_all(self, rows_to_insert, insert_iterations, all_rows_ids=None):
        table_path = f"{self._get_test_dir()}/testTableAllUpdated"
        self.ydb_client.query(f"DROP TABLE IF EXISTS `{table_path}`;")
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id1 Int32 NOT NULL,
                value Utf8 NOT NULL,
                PRIMARY KEY(id1),
            )
            WITH (
                STORE = COLUMN
            )
            """
        )
        if all_rows_ids is None:
            all_rows_ids = random.sample(range(self.MAX_ID), rows_to_insert * insert_iterations)
        rows_in_table = 0
        for it in range(0, len(all_rows_ids), rows_to_insert):
            rows_ids = all_rows_ids[it:it + rows_to_insert]

            insert_query = f"INSERT INTO `{table_path}` (id1, value) VALUES "
            for i in rows_ids:
                insert_query += f"({i}, 'OLD'), "
            insert_query = insert_query[:-2] + ";"
            self.ydb_client.query(insert_query)

            rows_in_table += rows_to_insert
            assert self._get_row_count(table_path) == rows_in_table

        self.ydb_client.query(f"UPDATE `{table_path}` SET value = 'NEW';")

        assert self._get_row_count(table_path) == rows_in_table

        self._test_select_query(
            f"""
            SELECT id1 as id1, value as value FROM `{table_path}` WHERE id1 != 10000001 ORDER by id1 DESC;
            """,
            reversed=True
        )

        self._test_select_query(
            f"""
            SELECT id1 as id1, value as value FROM `{table_path}` WHERE id1 != 10000001 ORDER by id1 ASC;
            """,
            reversed=False
        )

        self._test_select_query(
            f"""
            SELECT id1 as id1, value as value FROM `{table_path}` WHERE id1 != 10000001 ORDER by id1 ASC LIMIT 100;
            """,
            reversed=False
        )

        self._test_select_query(
            f"""
            SELECT id1 as id1, value as value FROM `{table_path}` WHERE id1 != 10000001 ORDER by id1 DESC LIMIT 100;
            """,
            reversed=True
        )

    def test_delete_all_rows_after_several_inserts(self):
        # IMPORTANT note: tests passes with 1 insert_iterations
        self._test_delete_all(rows_to_insert=1000, insert_iterations=2)

    def test_update_all_rows_after_several_inserts(self):
        # IMPORTANT note: tests passes with 1 insert_iterations
        self._test_update_all(rows_to_insert=1000, insert_iterations=2)
        # Known failing scenarios:
        self._test_update_all(rows_to_insert=2, insert_iterations=2, all_rows_ids=[2125791724, 269299117, 1929453799, 383764224])
        self._test_update_all(rows_to_insert=2, insert_iterations=2, all_rows_ids=[945294066, 1142679124, 175083513, 1218666029])
        self._test_update_all(rows_to_insert=10, insert_iterations=2,
                              all_rows_ids=[1979586701, 127598743, 231258081, 416119681, 1889436625, 303117720, 155720914, 673430720,
                                            1732479562, 17223092, 2089986107, 1847206032, 1598948818, 1418309310, 586602536, 630824296,
                                            434780620, 1254100566, 525597905, 860984260])
