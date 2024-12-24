# -*- coding: utf-8 -*-
import os
import pytest
import ydb

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure


class TestYdbKvWorkload(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(erasure=Erasure.NONE))
        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint="%s:%s" % (
                    cls.cluster.nodes[1].host, cls.cluster.nodes[1].port
                )
            )
        )
        cls.driver.wait()
        cls.pool = ydb.QuerySessionPool(cls.driver)

    @classmethod
    def teardown_class(cls):
        cls.pool.stop()
        cls.driver.stop()
        cls.cluster.stop()

    def setup_method(self):
        current_test_full_name = os.environ.get("PYTEST_CURRENT_TEST")
        self.table_path = "table_" + current_test_full_name.replace("::", ".").removesuffix(" (setup)")
        print(self.table_path)

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

        table_name = table_name = "{}/{}_{}_{}".format(self.table_path, type_, value, is_column)

        table_definition = f"""
                CREATE TABLE `{table_name}` (
                id Int64 NOT NULL,
                value {type_} NOT NULL,
                PRIMARY KEY (id)
            ) """

        if is_column:
            table_definition += " PARTITION BY HASH(id) WITH(STORE=COLUMN)"

            self.pool.execute_with_retries(
                table_definition
            )

        self.pool.execute_with_retries(
            f"""
                UPSERT INTO `{table_name}` (id, value) VALUES (1, {value});
            """
        )

        result = self.pool.execute_with_retries(
            f"""
                SELECT id, value FROM `{table_name}` WHERE id = 1;
            """
        )

        rows = result[0].rows
        assert len(rows) == 1, "Expected one row"
        assert rows[0].id == 1, "ID does not match"
        assert rows[0].value == value, "Value does not match"


    def test_dynumber(self):
        table_name = "{}/{}".format(self.table_path, "dynamber")
        self.pool.execute_with_retries(
            f"""
                CREATE TABLE `{table_name}` (
                id DyNumber,
                PRIMARY KEY (id)
            );"""
        )

        self.pool.execute_with_retries(
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

        result = self.pool.execute_with_retries(
            f"""
                SELECT count(*) FROM `{table_name}`;
            """
        )

        assert result[0].rows[0][0] == 4
