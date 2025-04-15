# -*- coding: utf-8 -*-
from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.library.test_meta import link_test_case

class TestYdbFilterDate(TestBase):
    @link_test_case("#17186")
    def test_crud_operations(self):
        table_name = f"{self.table_path}"

        table_definition = f"""
            CREATE TABLE `{table_name}` (
                id Int64 NOT NULL,
                date Date,
                PRIMARY KEY (id)
            )
            WITH (
                STORE = COLUMN
            )
        """

        self.query(table_definition)

        # # Create (Insert)
        self.query(
            f"""
            UPSERT INTO `{table_name}` (id, date) VALUES (42, CAST('2001-01-01' AS Date));
            """
        )

        rows = self.query(
            f"""
            SELECT * FROM `{table_name}`
            WHERE date <= CAST('2081-01-01' AS Date)
            """
        )
        assert len(rows) == 1
