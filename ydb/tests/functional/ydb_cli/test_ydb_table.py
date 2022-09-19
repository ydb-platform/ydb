# -*- coding: utf-8 -*-

from ydb.tests.library.common import yatest_common
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
import ydb
import os
import logging


logger = logging.getLogger(__name__)


def ydb_bin():
    return yatest_common.binary_path("ydb/apps/ydb/ydb")


def upsert_simple(session, full_path):
    path, table = os.path.split(full_path)
    session.transaction().execute(
        """
        PRAGMA TablePathPrefix("{0}");
        UPSERT INTO {1} (`key`, `id`, `value`) VALUES (1, 1111, "one");
        UPSERT INTO {1} (`key`, `id`, `value`) VALUES (2, 2222, "two");
        UPSERT INTO {1} (`key`, `id`, `value`) VALUES (3, 3333, "three");
        UPSERT INTO {1} (`key`, `id`, `value`) VALUES (5, 5555, "five");
        UPSERT INTO {1} (`key`, `id`, `value`) VALUES (7, 7777, "seven");
        """.format(path, table),
        commit_tx=True,
    )


def create_table_with_data(session, path):
    session.create_table(
        path,
        ydb.TableDescription()
        .with_column(ydb.Column("key", ydb.OptionalType(ydb.PrimitiveType.Uint32)))
        .with_column(ydb.Column("id", ydb.OptionalType(ydb.PrimitiveType.Uint64)))
        .with_column(ydb.Column("value", ydb.OptionalType(ydb.PrimitiveType.String)))
        .with_primary_keys("key")
    )

    upsert_simple(session, path)


class BaseTestTableService(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()
        cls.root_dir = "/Root"
        driver_config = ydb.DriverConfig(
            database="/Root",
            endpoint="%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].port))
        cls.driver = ydb.Driver(driver_config)
        cls.driver.wait(timeout=4)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    @classmethod
    def execute_ydb_cli_command(cls, args):
        execution = yatest_common.execute(
            [
                ydb_bin(),
                "--endpoint", "grpc://localhost:%d" % cls.cluster.nodes[1].grpc_port,
                "--database", cls.root_dir
            ] +
            args
        )

        result = execution.std_out
        logger.debug("std_out:\n" + result.decode('utf-8'))
        return result

    @staticmethod
    def canonical_result(output_result):
        output_file_name = "result.output"
        with open(output_file_name, "w") as f:
            f.write(output_result.decode('utf-8'))
        return yatest_common.canonical_file(output_file_name, local=True, universal_lines=True)


class TestExecuteQueryWithParams(BaseTestTableService):

    @classmethod
    def setup_class(cls):
        BaseTestTableService.setup_class()

        session = cls.driver.table_client.session().create()
        cls.table_path = cls.root_dir + "/table_params"
        create_table_with_data(session, cls.table_path)

    def test_uint32(self):
        query = "DECLARE $par1 AS Uint32; SELECT * FROM `{}` WHERE key = $par1;".format(self.table_path)
        output = self.execute_ydb_cli_command(["table", "query", "execute", "-q", query, "--param", "$par1=1"])
        return self.canonical_result(output)

    def test_uint64_and_string(self):
        query = "DECLARE $id AS Uint64; "\
                "DECLARE $value AS String; "\
                "SELECT * FROM `{}` WHERE id = $id OR value = $value;".format(self.table_path)
        output = self.execute_ydb_cli_command(["table", "query", "execute", "-q", query, "--param", "$id=2222",
                                               "--param", "$value=\"seven\""])
        return self.canonical_result(output)

    def test_list(self):
        query = "DECLARE $values AS List<Uint64?>; SELECT $values AS values;"
        output = self.execute_ydb_cli_command(["table", "query", "execute", "-q", query, "--param", "$values=[1,2,3]"])
        return self.canonical_result(output)

    def test_struct(self):
        query = "DECLARE $values AS List<Struct<key:Uint64, value:Utf8>>; "\
                "SELECT "\
                "Table.key AS key, "\
                "Table.value AS value "\
                "FROM (SELECT $values AS lst) FLATTEN BY lst AS Table;"
        output = self.execute_ydb_cli_command(["table", "query", "execute", "-q", query, "--param",
                                               "$values=[{\"key\":1,\"value\":\"one\"},{\"key\":2,\"value\":\"two\"}]"])
        return self.canonical_result(output)

    def test_scan_query_with_parameters(self):
        query = "DECLARE $id AS Uint64; "\
                "DECLARE $value AS String; "\
                "SELECT * FROM `{}` WHERE id = $id OR value = $value;".format(self.table_path)
        output = self.execute_ydb_cli_command(["table", "query", "execute", "-t", "scan", "-q", query,
                                               "--param", "$id=2222",
                                               "--param", "$value=\"seven\""])
        return self.canonical_result(output)


class TestExecuteQueryWithFormats(BaseTestTableService):

    @classmethod
    def setup_class(cls):
        BaseTestTableService.setup_class()

        session = cls.driver.table_client.session().create()
        cls.table_path = cls.root_dir + "/table_formats"
        create_table_with_data(session, cls.table_path)

    def execute_data_query(self, format):
        query = "SELECT * FROM `{}` WHERE key < 4;".format(self.table_path)
        output = self.execute_ydb_cli_command(["table", "query", "execute", "-t", "data", "-q", query,
                                               "--format", format])
        return self.canonical_result(output)

    def execute_scan_query(self, format):
        query = "SELECT * FROM `{}` WHERE key < 4;".format(self.table_path)
        output = self.execute_ydb_cli_command(["table", "query", "execute", "-t", "scan", "-q", query,
                                               "--format", format])
        return self.canonical_result(output)

    def execute_read_table(self, format):
        output = self.execute_ydb_cli_command(["table", "readtable", self.table_path, "--format", format])
        return self.canonical_result(output)

    # DataQuery

    def test_data_query_pretty(self):
        return self.execute_data_query('pretty')

    def test_data_query_json_base64(self):
        return self.execute_data_query('json-base64')

    def test_data_query_json_base64_array(self):
        return self.execute_data_query('json-base64-array')

    def test_data_query_json_unicode(self):
        return self.execute_data_query('json-unicode')

    def test_data_query_json_unicode_array(self):
        return self.execute_data_query('json-unicode-array')

    def test_data_query_csv(self):
        return self.execute_data_query('csv')

    def test_data_query_tsv(self):
        return self.execute_data_query('tsv')

    # ScanQuery

    def test_scan_query_pretty(self):
        return self.execute_scan_query('pretty')

    def test_scan_query_json_base64(self):
        return self.execute_scan_query('json-base64')

    def test_scan_query_json_base64_array(self):
        return self.execute_scan_query('json-base64-array')

    def test_scan_query_json_unicode(self):
        return self.execute_scan_query('json-unicode')

    def test_scan_query_json_unicode_array(self):
        return self.execute_scan_query('json-unicode-array')

    def test_scan_query_csv(self):
        return self.execute_scan_query('csv')

    def test_scan_query_tsv(self):
        return self.execute_scan_query('tsv')

    # ReadTable

    def test_read_table_pretty(self):
        return self.execute_read_table('pretty')

    def test_read_table_json_base64(self):
        return self.execute_read_table('json-base64')

    def test_read_table_json_base64_array(self):
        return self.execute_read_table('json-base64-array')

    def test_read_table_json_unicode(self):
        return self.execute_read_table('json-unicode')

    def test_read_table_json_unicode_array(self):
        return self.execute_read_table('json-unicode-array')

    def test_read_table_csv(self):
        return self.execute_read_table('csv')

    def test_read_table_tsv(self):
        return self.execute_read_table('tsv')
