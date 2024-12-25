# -*- coding: utf-8 -*-
import pytest
from .test_base import TestBase


class TestYdbKvWorkload(TestBase):

    @pytest.mark.parametrize("is_column", [True, False])
    @pytest.mark.parametrize("type_,value",
                             [
                                 ("Int32", -2 ** 31),
                                 ("Int32", 2 ** 31 - 1),
                                 ("UInt32", 0),
                                 ("UInt32", 2 ** 32 - 1),
                                 ("Int64", -2 ** 63),
                                 ("Int64", 2 ** 63 - 1),
                                 ("Uint64", 0),
                                 ("Uint64", 2 ** 64 - 1)
                                 ])
    def test_minimal_maximal_values(self, is_column, type_, value):
        """
        Test verifies correctness of handling minimal and maximal values for types
        """

        # table_name = table_name = "{}/{}_{}_{}".format(self.table_path, type_, value, is_column)
        table_name = f"{self.table_path}"

        table_definition = f"""
                CREATE TABLE `{table_name}` (
                id Int64 NOT NULL,
                value {type_} NOT NULL,
                PRIMARY KEY (id)
            ) """

        if is_column:
            table_definition += " PARTITION BY HASH(id) WITH(STORE=COLUMN)"

        self.query(table_definition)

        self.query(
            f"""
                UPSERT INTO `{table_name}` (id, value) VALUES (1, {value});
            """
        )

        return self.query(
            f"""
                SELECT id, value FROM `{table_name}` WHERE id = 1;
            """
        )

    def test_dynumber(self):
        table_name = "{}/{}".format(self.table_path, "dynamber")
        self.query(
            f"""
                CREATE TABLE `{table_name}` (
                id DyNumber,
                PRIMARY KEY (id)
            );"""
        )

        self.query(
            f"""
                UPSERT INTO `{table_name}` (id)
                VALUES
                    (DyNumber(".149e4")),
                    (DyNumber("15e2")),
                    (DyNumber("150e1")),  -- same as 15e2
                    (DyNumber("151e4")),
                    (DyNumber("1500.1"));
            """
        )

        return self.query(
            f"""
                SELECT count(*) FROM `{table_name}`;
            """
        )
