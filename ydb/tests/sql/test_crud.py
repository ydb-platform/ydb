# -*- coding: utf-8 -*-
from .test_base import TestBase


class TestYdbCrudOperations(TestBase):

    def test_crud_operations(self):
        """
        Test basic CRUD operations: Create, Read, Update, Delete
        """

        table_name = f"{self.table_path}"

        table_definition = f"""
            CREATE TABLE `{table_name}` (
                id Int64 NOT NULL,
                value Utf8 NOT NULL,
                PRIMARY KEY (id)
            )
            PARTITION BY HASH(id)
            WITH(STORE=COLUMN)
            """

        self.query(table_definition)

        # Create (Insert)
        self.query(
            f"""
            UPSERT INTO `{table_name}` (id, value) VALUES (1, 'initial_value');
            """
        )

        # Read
        rows = self.query(
            f"""
            SELECT value FROM `{table_name}` WHERE id = 1;
            """
        )
        assert len(rows) == 1, "Expected one row after insert"
        assert rows[0].value == 'initial_value', "Value mismatch for inserted data"

        # Update
        self.query(
            f"""
            UPDATE `{table_name}` SET value = 'updated_value' WHERE id = 1;
            """
        )
        rows = self.query(
            f"""
            SELECT value FROM `{table_name}` WHERE id = 1;
            """
        )
        assert len(rows) == 1, "Expected one row after update"
        assert rows[0].value == 'updated_value', "Value mismatch after update"

        # Delete
        self.query(
            f"""
            DELETE FROM `{table_name}` WHERE id = 1;
            """
        )
        rows = self.query(
            f"""
            SELECT value FROM `{table_name}` WHERE id = 1;
            """
        )
        assert len(rows) == 0, "Expected no rows after delete"
