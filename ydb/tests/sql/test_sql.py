# -*- coding: utf-8 -*-
import os

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

    def setup(self):
        current_test_full_name = os.environ.get("PYTEST_CURRENT_TEST")
        self.table_path = "table_" + current_test_full_name.replace("::", ".").removesuffix(" (setup)")
        print(self.table_path)

    def test_minimal_maximal_values(self):
        """
        Test verifies correctness of handling minimal and maximal values for types
        """

        type_to_values_to_check = {
            "Int32": [-2 ** 31, 2 ** 31 - 1],
            "Uint32": [0, 2 ** 32 - 1],
            "Int64": [-2 ** 63, 2 ** 63 - 1],
            "Uint64": [0, 2 ** 64 - 1],
        }

        for type_, values in type_to_values_to_check.items():
            for i, value in enumerate(values):
                table_name = table_name = "{}/{}_{}".format(self.table_path, type_, i)

                self.pool.execute_with_retries(
                    f"""
                        CREATE TABLE `{table_name}` (
                        id Int64,
                        value {type_},
                        PRIMARY KEY (id)
                    );"""
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
