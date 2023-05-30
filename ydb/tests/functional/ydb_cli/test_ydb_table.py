# -*- coding: utf-8 -*-

from ydb.tests.library.common import yatest_common
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.oss.canonical import set_canondata_root
from ydb.tests.oss.ydb_sdk_import import ydb

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
        set_canondata_root('ydb/tests/functional/ydb_cli/canondata')

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
    def execute_ydb_cli_command(cls, args, stdin=None):
        execution = yatest_common.execute(
            [
                ydb_bin(),
                "--endpoint", "grpc://localhost:%d" % cls.cluster.nodes[1].grpc_port,
                "--database", cls.root_dir
            ] +
            args, stdin=stdin
        )

        result = execution.std_out
        logger.debug("std_out:\n" + result.decode('utf-8'))
        return result

    @staticmethod
    def canonical_result(output_result):
        output_file_name = "table_result.output"
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


class TestExecuteQueryWithParamsFromJson(BaseTestTableService):
    @classmethod
    def setup_class(cls):
        BaseTestTableService.setup_class()

        session = cls.driver.table_client.session().create()
        cls.table_path = cls.root_dir + "/table_params_from_json"
        create_table_with_data(session, cls.table_path)

    @staticmethod
    def write_data(data, filename="table_params.json"):
        with open(filename, "w") as file:
            file.write(data)

    def uint32(self, query_type):
        param_data = '{\n' \
            '   "par1": 1\n' \
            '}'
        query = "DECLARE $par1 AS Uint32; SELECT * FROM `{}` WHERE key = $par1;".format(self.table_path)
        self.write_data(param_data)
        output = self.execute_ydb_cli_command(
            ["table", "query", "execute", "-t", query_type, "--param-file", "table_params.json", "-q", query]
        )
        return self.canonical_result(output)

    def uint64_and_string(self, query_type):
        param_data = '{\n' \
            '   "value": "seven",\n' \
            '   "id": 2222' \
            '}'
        query = "DECLARE $id AS Uint64; "\
                "DECLARE $value AS String; "\
                "SELECT * FROM `{}` WHERE id = $id OR value = $value;".format(self.table_path)
        self.write_data(param_data)
        output = self.execute_ydb_cli_command(
            ["table", "query", "execute", "-t", query_type, "--param-file", "table_params.json", "-q", query]
        )
        return self.canonical_result(output)

    def list(self, query_type):
        param_data = '{\n' \
            '   "values": [1, 2, 3]\n' \
            '}'
        query = "DECLARE $values AS List<Uint64?>; SELECT $values AS values;"
        self.write_data(param_data)
        output = self.execute_ydb_cli_command(
            ["table", "query", "execute", "-t", query_type, "--param-file", "table_params.json", "-q", query]
        )
        return self.canonical_result(output)

    def struct(self, query_type):
        param_data = '{\n' \
            '   "values": [\n' \
            '       {\n'\
            '           "key": 1,\n'\
            '           "value": "one"\n'\
            '       },\n'\
            '       {\n'\
            '           "key": 2,\n'\
            '           "value": "two"\n'\
            '       }\n'\
            '   ]\n'\
            '}'
        query = "DECLARE $values AS List<Struct<key:Uint64, value:Utf8>>; "\
                "SELECT "\
                "Table.key AS key, "\
                "Table.value AS value "\
                "FROM (SELECT $values AS lst) FLATTEN BY lst AS Table;"
        self.write_data(param_data)
        output = self.execute_ydb_cli_command(
            ["table", "query", "execute", "-t", query_type, "--param-file", "table_params.json", "-q", query]
        )
        return self.canonical_result(output)

    def multiple_files(self, query_type):
        param_data1 = '{\n' \
            '   "str": "Строчка"\n' \
            '}'
        param_data2 = '{\n' \
            '   "num": 1542\n' \
            '}'
        param_data3 = '{\n' \
            '   "date": "2011-11-11"\n' \
            '}'
        query = "DECLARE $str AS Utf8; "\
                "DECLARE $num AS Uint64; "\
                "DECLARE $date AS Date; "\
                "SELECT $str AS str, $num as num, $date as date; "
        self.write_data(param_data1, "table_param1.json")
        self.write_data(param_data2, "table_param2.json")
        self.write_data(param_data3, "table_param3.json")
        output = self.execute_ydb_cli_command(
            ["table", "query", "execute", "-t", query_type, "--param-file", "table_param1.json", "--param-file",
             "table_param2.json", "--param-file", "table_param3.json", "-q", query]
        )
        return self.canonical_result(output)

    def ignore_excess_parameters(self, query_type):
        param_data = '{\n' \
            '   "a": 12,\n' \
            '   "b": 34' \
            '}'
        query = "DECLARE $a AS Uint64; " \
                "SELECT $a AS a; "
        self.write_data(param_data)
        output = self.execute_ydb_cli_command(
            ["table", "query", "execute", "-t", query_type, "-q", query, "--param-file", "table_params.json"]
        )
        return self.canonical_result(output)

    def script_from_file(self, query_type):
        query = "DECLARE $a AS Uint64; " \
                "SELECT $a AS a; "
        self.write_data(query, "query.yql")
        output = self.execute_ydb_cli_command(
            ["table", "query", "execute", "-t", query_type, "-f", "query.yql", "--param", "$a=3"]
        )
        return self.canonical_result(output)

    def test_data_query_uint32(self):
        return self.uint32("data")

    def test_data_query_uint64_and_string(self):
        return self.uint64_and_string("data")

    def test_data_query_list(self):
        return self.list("data")

    def test_data_query_struct(self):
        return self.struct("data")

    def test_data_query_multiple_files(self):
        return self.multiple_files("data")

    def test_data_ignore_excess_parameters(self):
        return self.ignore_excess_parameters("data")

    def test_data_query_script_from_file(self):
        return self.script_from_file("data")

    def test_scan_query_uint32(self):
        return self.uint32("scan")

    def test_scan_query_uint64_and_string(self):
        return self.uint64_and_string("scan")

    def test_scan_query_list(self):
        return self.list("scan")

    def test_scan_query_struct(self):
        return self.struct("scan")

    def test_scan_query_multiple_files(self):
        return self.multiple_files("scan")

    def test_scan_ignore_excess_parameters(self):
        return self.ignore_excess_parameters("scan")

    def test_scan_query_script_from_file(self):
        return self.script_from_file("scan")


class TestExecuteQueryWithParamsFromStdin(BaseTestTableService):
    @classmethod
    def setup_class(cls):
        BaseTestTableService.setup_class()

        session = cls.driver.table_client.session().create()
        cls.table_path = cls.root_dir + "/table_params_from_stdin"
        create_table_with_data(session, cls.table_path)

    @staticmethod
    def write_data(data, filename="table_stdin.txt"):
        with open(filename, "w") as file:
            file.write(data)

    @classmethod
    def get_stdin(cls):
        cls.stdin = open("table_stdin.txt", "r")
        return cls.stdin

    @classmethod
    def close_stdin(cls):
        cls.stdin.close()

    def simple_json(self, query_type):
        param_data = '{\n' \
            '   "s": "Some_string",\n' \
            '   "val": 32\n' \
            '}'
        query = "DECLARE $s AS Utf8; "\
                "DECLARE $val AS Uint64; "\
                "SELECT $s AS s, $val AS val; "
        self.write_data(param_data)
        output = self.execute_ydb_cli_command(["table", "query", "execute", "-t", query_type, "-q", query], self.get_stdin())
        self.close_stdin()
        return self.canonical_result(output)

    def text_data(self, query_type):
        param_data = 'Line1\n' \
            'Line2\n' \
            'Line3\n'
        query = "DECLARE $s AS Utf8; " \
                "SELECT $s AS s; "
        self.write_data(param_data)
        output = self.execute_ydb_cli_command(
            ["table", "query", "execute", "-t", query_type, "-q", query, "--stdin-format", "raw", "--stdin-par", "s"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output)

    def unnamed_json(self, query_type):
        param_data = "[1, 2, 3, 4]"
        query = "DECLARE $arr AS List<Uint64>; "\
                "SELECT $arr AS arr; "
        self.write_data(param_data)
        output = self.execute_ydb_cli_command(
            ["table", "query", "execute", "-t", query_type, "-q", query, "--stdin-par", "arr"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output)

    def mix_json_and_binary(self, query_type):
        param_data1 = 'Строка номер 1\n' \
            'String number 2\n' \
            'Строка номер 3'
        param_data2 = '{\n' \
            '   "date": "2044-08-21",\n' \
            '   "val": 32\n' \
            '}'
        query = "DECLARE $s AS String; " \
                "DECLARE $date AS Date; " \
                "DECLARE $val AS Uint64; " \
                "SELECT $s AS s, $date AS date, $val AS val; "
        self.write_data(param_data1)
        self.write_data(param_data2, "table_params.json")
        output = self.execute_ydb_cli_command(
            ["table", "query", "execute", "-t", query_type, "-q", query, "--stdin-par", "s",
             "--stdin-format", "raw", "--param-file", "table_params.json"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output)

    def different_sources(self, query_type):
        param_data1 = '{\n' \
            '   "s": "Строка utf-8"\n' \
            '}'
        param_data2 = '{\n' \
            '   "date": "2000-09-01"\n' \
            '}'
        query = "DECLARE $s AS Utf8; " \
                "DECLARE $date AS Date; " \
                "DECLARE $val AS Uint64; " \
                "SELECT $s AS s, $date AS date, $val AS val; "
        self.write_data(param_data1)
        self.write_data(param_data2, "table_params.json")
        output = self.execute_ydb_cli_command(
            ["table", "query", "execute", "-t", query_type, "-q", query, "--param-file", "table_params.json", "--param", "$val=100"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output)

    def framing_newline_delimited(self, query_type):
        param_data = '{"s": "Some text", "num": 1}\n' \
            '{"s": "Строка 1\\nСтрока2", "num": 2}\n' \
            '{"s": "Abacaba", "num": 3}\n'
        query = "DECLARE $s AS Utf8; " \
                "DECLARE $num AS Uint64; " \
                "SELECT $s AS s, $num AS num; "
        self.write_data(param_data)
        output = self.execute_ydb_cli_command(
            ["table", "query", "execute", "-t", query_type, "-q", query, "--stdin-format", "newline-delimited"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output)

    def text_data_framing(self, query_type):
        param_data = 'Line1\n' \
            'Line2\n' \
            'Line3\n'
        query = "DECLARE $s AS Utf8; " \
                "SELECT $s AS s; "
        self.write_data(param_data)
        output = self.execute_ydb_cli_command(
            ["table", "query", "execute", "-t", query_type, "-q", query, "--stdin-format", "raw",
             "--stdin-par", "s", "--stdin-format", "newline-delimited"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output)

    def batching_full(self, query_type):
        param_data = 'Line1\n' \
            'Line2\n' \
            'Line3\n'
        query = "DECLARE $s AS List<Utf8>; " \
                "SELECT $s AS s; "
        self.write_data(param_data)
        output = self.execute_ydb_cli_command(
            ["table", "query", "execute", "-t", query_type, "-q", query, "--stdin-format", "raw", "--stdin-par", "s",
             "--stdin-format", "newline-delimited", "--batch", "full"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output)

    def batching_adaptive(self, query_type):
        param_data = '{"s": "Line1", "id": 1}\n' \
            '{"s": "Line2", "id": 2}\n' \
            '{"s": "Line3", "id": 3}\n' \
            '{"s": "Line4", "id": 4}\n' \
            '{"s": "Line5", "id": 5}\n' \
            '{"s": "Line6", "id": 6}\n' \
            '{"s": "Line7", "id": 7}\n' \
            '{"s": "Line8", "id": 8}\n' \
            '{"s": "Line9", "id": 9}\n'
        query = "DECLARE $arr as List<Struct<s:Utf8, id:Uint64>>; " \
                "SELECT $arr as arr; "
        self.write_data(param_data)
        output = self.execute_ydb_cli_command(
            ["table", "query", "execute", "-t", query_type, "-q", query, "--stdin-par", "arr", "--stdin-format", "newline-delimited",
             "--batch", "adaptive", "--batch-max-delay", "0", "--batch-limit", "3"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output)

    def ignore_excess_parameters(self, query_type):
        param_data = '{\n' \
            '   "a": 12,\n' \
            '   "b": 34' \
            '}'
        query = "DECLARE $a AS Uint64; " \
                "SELECT $a AS a; "
        self.write_data(param_data)
        output = self.execute_ydb_cli_command(
            ["table", "query", "execute", "-t", query_type, "-q", query],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output)

    def test_data_query_simple_json(self):
        return self.simple_json("data")

    def test_data_query_text_data(self):
        return self.text_data("data")

    def test_data_query_unnamed_json(self):
        return self.unnamed_json("data")

    def test_data_query_mix_json_and_binary(self):
        return self.mix_json_and_binary("data")

    def test_data_query_different_sources(self):
        return self.different_sources("data")

    def test_data_query_framing_newline_delimited(self):
        return self.framing_newline_delimited("data")

    def test_data_query_text_data_framing(self):
        return self.text_data_framing("data")

    def test_data_query_batching_full(self):
        return self.batching_full("data")

    def test_data_query_batching_adaptive(self):
        return self.batching_adaptive("data")

    def test_data_ignore_excess_parameters(self):
        return self.ignore_excess_parameters("data")

    def test_scan_query_simple_json(self):
        return self.simple_json("scan")

    def test_scan_query_text_data(self):
        return self.text_data("scan")

    def test_scan_query_unnamed_json(self):
        return self.unnamed_json("scan")

    def test_scan_query_mix_json_and_binary(self):
        return self.mix_json_and_binary("scan")

    def test_scan_query_different_sources(self):
        return self.different_sources("scan")

    def test_scan_query_framing_newline_delimited(self):
        return self.framing_newline_delimited("scan")

    def test_scan_query_text_data_framing(self):
        return self.text_data_framing("scan")

    def test_scan_query_batching_full(self):
        return self.batching_full("scan")

    def test_scan_query_batching_adaptive(self):
        return self.batching_adaptive("scan")

    def test_scan_ignore_excess_parameters(self):
        return self.ignore_excess_parameters("scan")
