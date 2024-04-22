#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

from ydb.tests.oss.ydb_sdk_import import ydb


def _list_operations_request(kind):
    return ydb._apis.ydb_operation.ListOperationsRequest(kind=kind)


def list_operations(driver):
    return driver(_list_operations_request("scriptexec"),
                  ydb._apis.OperationService.Stub,
                  "ListOperations")


columns = ["syntax", "ast", "stats"]


class TestUpdateScriptTablesYdb(object):
    @classmethod
    def setup_class(cls):
        cls.config = KikimrConfigGenerator(extra_feature_flags=["enable_script_execution_operations"])
        cls.cluster = kikimr_cluster_factory(configurator=cls.config)
        cls.cluster.start()
        cls.driver = ydb.Driver(ydb.DriverConfig(
            database="/Root",
            endpoint="%s:%d" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port)))
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def check_table_existance(self, table_name):
        with self.session.transaction() as tx:
            tx.execute(
                "SELECT * FROM `.metadata/script_executions`"
            )
            tx.commit()

    drop_columns = ["ALTER TABLE {} " + ', '.join(["DROP COLUMN {}".format(col) for col in columns[:i + 1]]) for i in range(len(columns))]

    @pytest.mark.parametrize("table_name", ["`.metadata/script_executions`"])
    @pytest.mark.parametrize("operation", ["DROP TABLE {}", *drop_columns])
    def test_recreate_tables(self, operation, table_name):
        self.session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        res = list_operations(self.driver)
        assert "SUCCESS" in str(res)

        self.check_table_existance(table_name)

        self.session.execute_scheme(
            operation.format(table_name)
        )

        res = list_operations(self.driver)
        assert "ERROR" in str(res)

        self.cluster.stop()
        self.cluster.start()
        self.session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())

        res = list_operations(self.driver)
        assert "SUCCESS" in str(res)

        self.check_table_existance(table_name)
