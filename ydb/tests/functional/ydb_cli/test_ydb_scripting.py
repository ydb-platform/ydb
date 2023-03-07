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


class BaseTestScriptingService(object):
    @classmethod
    def execute_ydb_cli_command(cls, args, stdin=None):
        execution = yatest_common.execute([ydb_bin()] + args, stdin=stdin)
        result = execution.std_out
        logger.debug("std_out:\n" + result.decode('utf-8'))
        return result

    @staticmethod
    def canonical_result(output_result):
        output_file_name = "result.output"
        with open(output_file_name, "w") as f:
            f.write(output_result.decode('utf-8'))
        return yatest_common.canonical_file(output_file_name, local=True, universal_lines=True)


class BaseTestScriptingServiceWithDatabase(BaseTestScriptingService):
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
        cls.cluster.stop()

    @classmethod
    def execute_ydb_cli_command_with_db(cls, args, stdin=None):
        return cls.execute_ydb_cli_command(
            [
                "--endpoint", "grpc://localhost:%d" % cls.cluster.nodes[1].grpc_port,
                "--database", cls.root_dir
            ] +
            args, stdin
        )


class TestExecuteScriptWithParams(BaseTestScriptingServiceWithDatabase):
    @classmethod
    def setup_class(cls):
        BaseTestScriptingServiceWithDatabase.setup_class()

        session = cls.driver.table_client.session().create()
        cls.table_path = cls.root_dir + "/scripting_params"
        create_table_with_data(session, cls.table_path)

    def test_uint32(self):
        script = "DECLARE $par1 AS Uint32; SELECT * FROM `{}` WHERE key = $par1;".format(self.table_path)
        output = self.execute_ydb_cli_command_with_db(["scripting", "yql", "-s", script, "--param", "$par1=1"])
        return self.canonical_result(output)

    def test_uint64_and_string(self):
        script = "DECLARE $id AS Uint64; "\
                 "DECLARE $value AS String; "\
                 "SELECT * FROM `{}` WHERE id = $id OR value = $value;".format(self.table_path)
        output = self.execute_ydb_cli_command_with_db(
            ["scripting", "yql", "-s", script, "--param", "$id=2222",
             "--param", "$value=\"seven\""]
        )
        return self.canonical_result(output)

    def test_list(self):
        script = "DECLARE $values AS List<Uint64?>; SELECT $values AS values;"
        output = self.execute_ydb_cli_command_with_db(["scripting", "yql", "-s", script, "--param", "$values=[1,2,3]"])
        return self.canonical_result(output)

    def test_struct(self):
        script = "DECLARE $values AS List<Struct<key:Uint64, value:Utf8>>; "\
                 "SELECT "\
                 "Table.key AS key, "\
                 "Table.value AS value "\
                 "FROM (SELECT $values AS lst) FLATTEN BY lst AS Table;"
        output = self.execute_ydb_cli_command_with_db(
            ["scripting", "yql", "-s", script, "--param",
             "$values=[{\"key\":1,\"value\":\"one\"},{\"key\":2,\"value\":\"two\"}]"]
        )
        return self.canonical_result(output)


class TestScriptingServiceHelp(BaseTestScriptingService):
    def test_help(self):
        output = self.execute_ydb_cli_command(["scripting", "yql", "--help"])
        return self.canonical_result(output)

    def test_help_ex(self):
        output = self.execute_ydb_cli_command(["scripting", "yql", "--help-ex"])
        return self.canonical_result(output)


class TestExecuteScriptWithFormats(BaseTestScriptingServiceWithDatabase):
    @classmethod
    def setup_class(cls):
        BaseTestScriptingServiceWithDatabase.setup_class()

        session = cls.driver.table_client.session().create()
        cls.table_path = cls.root_dir + "/scripting_formats"
        create_table_with_data(session, cls.table_path)

    def yql_script(self, format):
        script = "SELECT * FROM `{path}` WHERE key < 4;" \
            "SELECT id FROM `{path}` WHERE key = 4;" \
            "SELECT value FROM `{path}` WHERE key > 5".format(path=self.table_path)
        output = self.execute_ydb_cli_command_with_db(["scripting", "yql", "-s", script, "--format", format])
        return self.canonical_result(output)

    def stream_yql_script(self, format):
        script = "SELECT * FROM `{path}` WHERE key < 4;" \
            "SELECT id FROM `{path}` WHERE key = 4;" \
            "SELECT value FROM `{path}` WHERE key > 5".format(path=self.table_path)
        output = self.execute_ydb_cli_command_with_db(["yql", "-s", script, "--format", format])
        return self.canonical_result(output)

    # YqlScript

    def test_yql_script_pretty(self):
        return self.yql_script('pretty')

    def test_yql_script_json_base64(self):
        return self.yql_script('json-base64')

    def test_yql_script_json_base64_array(self):
        return self.yql_script('json-base64-array')

    def test_yql_script_json_unicode(self):
        return self.yql_script('json-unicode')

    def test_yql_script_json_unicode_array(self):
        return self.yql_script('json-unicode-array')

    # StreamYqlScript

    def test_stream_yql_script_pretty(self):
        return self.stream_yql_script('pretty')

    def test_stream_yql_script_json_base64(self):
        return self.stream_yql_script('json-base64')

    def test_stream_yql_script_json_base64_array(self):
        return self.stream_yql_script('json-base64-array')

    def test_stream_yql_script_json_unicode(self):
        return self.stream_yql_script('json-unicode')

    def test_stream_yql_script_json_unicode_array(self):
        return self.stream_yql_script('json-unicode-array')


class TestExecuteScriptWithParamsFromJson(BaseTestScriptingServiceWithDatabase):
    @classmethod
    def setup_class(cls):
        BaseTestScriptingServiceWithDatabase.setup_class()

        session = cls.driver.table_client.session().create()
        cls.table_path = cls.root_dir + "/scripting_params_from_json"
        create_table_with_data(session, cls.table_path)

    @staticmethod
    def write_data(data, filename="params.json"):
        with open(filename, "w") as file:
            file.write(data)

    def uint32(self, command):
        param_data = '{\n' \
            '   "par1": 1\n' \
            '}'
        script = "DECLARE $par1 AS Uint32; SELECT * FROM `{}` WHERE key = $par1;".format(self.table_path)
        self.write_data(param_data)
        output = self.execute_ydb_cli_command_with_db(command + ["--param-file", "params.json", "-s", script])
        return self.canonical_result(output)

    def uint64_and_string(self, command):
        param_data = '{\n' \
            '   "value": "seven",\n' \
            '   "id": 2222' \
            '}'
        script = "DECLARE $id AS Uint64; "\
                 "DECLARE $value AS String; "\
                 "SELECT * FROM `{}` WHERE id = $id OR value = $value;".format(self.table_path)
        self.write_data(param_data)
        output = self.execute_ydb_cli_command_with_db(command + ["--param-file", "params.json", "-s", script])
        return self.canonical_result(output)

    def list(self, command):
        param_data = '{\n' \
            '   "values": [1, 2, 3]\n' \
            '}'
        script = "DECLARE $values AS List<Uint64?>; SELECT $values AS values;"
        self.write_data(param_data)
        output = self.execute_ydb_cli_command_with_db(command + ["--param-file", "params.json", "-s", script])
        return self.canonical_result(output)

    def struct(self, command):
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
        script = "DECLARE $values AS List<Struct<key:Uint64, value:Utf8>>; "\
                 "SELECT "\
                 "Table.key AS key, "\
                 "Table.value AS value "\
                 "FROM (SELECT $values AS lst) FLATTEN BY lst AS Table;"
        self.write_data(param_data)
        output = self.execute_ydb_cli_command_with_db(command + ["--param-file", "params.json", "-s", script])
        return self.canonical_result(output)

    def multiple_files(self, command):
        param_data1 = '{\n' \
            '   "str": "Строчка"\n' \
            '}'
        param_data2 = '{\n' \
            '   "num": 1542\n' \
            '}'
        param_data3 = '{\n' \
            '   "date": "2011-11-11"\n' \
            '}'
        script = "DECLARE $str AS Utf8; "\
                 "DECLARE $num AS Uint64; "\
                 "DECLARE $date AS Date; "\
                 "SELECT $str AS str, $num as num, $date as date; "
        self.write_data(param_data1, "param1.json")
        self.write_data(param_data2, "param2.json")
        self.write_data(param_data3, "param3.json")
        output = self.execute_ydb_cli_command_with_db(
            command + ["--param-file", "param1.json", "--param-file", "param2.json", "--param-file", "param3.json", "-s", script]
        )
        return self.canonical_result(output)

    def ignore_excess_parameters(self, command):
        param_data = '{\n' \
            '   "a": 12,\n' \
            '   "b": 34' \
            '}'
        script = "DECLARE $a AS Uint64; " \
                 "SELECT $a AS a; "
        self.write_data(param_data)
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script, "--param-file", "params.json"]
        )
        return self.canonical_result(output)

    def script_from_file(self, command):
        script = "DECLARE $a AS Uint64; " \
                 "SELECT $a AS a; "
        self.write_data(script, "script.yql")
        output = self.execute_ydb_cli_command_with_db(
            command + ["-f", "script.yql", "--param", "$a=3"]
        )
        return self.canonical_result(output)

    def test_uint32(self):
        return self.uint32(["scripting", "yql"])

    def test_uint64_and_string(self):
        return self.uint64_and_string(["scripting", "yql"])

    def test_list(self):
        return self.list(["scripting", "yql"])

    def test_struct(self):
        return self.struct(["scripting", "yql"])

    def test_multiple_files(self):
        return self.multiple_files(["scripting", "yql"])

    def test_ignore_excess_parameters(self):
        return self.ignore_excess_parameters(["scripting", "yql"])

    def test_script_from_file(self):
        return self.script_from_file(["scripting", "yql"])

    def test_stream_uint32(self):
        return self.uint32(["yql"])

    def test_stream_uint64_and_string(self):
        return self.uint64_and_string(["yql"])

    def test_stream_list(self):
        return self.list(["yql"])

    def test_stream_struct(self):
        return self.struct(["yql"])

    def test_stream_multiple_files(self):
        return self.multiple_files(["yql"])

    def test_stream_ignore_excess_parameters(self):
        return self.ignore_excess_parameters(["yql"])

    def test_stream_script_from_file(self):
        return self.script_from_file(["yql"])


class TestExecuteScriptWithParamsFromStdin(BaseTestScriptingServiceWithDatabase):
    @classmethod
    def setup_class(cls):
        BaseTestScriptingServiceWithDatabase.setup_class()

        session = cls.driver.table_client.session().create()
        cls.table_path = cls.root_dir + "/scripting_params_from_stdin"
        create_table_with_data(session, cls.table_path)

    @staticmethod
    def write_data(data, filename="stdin.txt"):
        with open(filename, "w") as file:
            file.write(data)

    @classmethod
    def get_stdin(cls):
        cls.stdin = open("stdin.txt", "r")
        return cls.stdin

    @classmethod
    def close_stdin(cls):
        cls.stdin.close()

    def simple_json(self, command):
        param_data = '{\n' \
            '   "s": "Some_string",\n' \
            '   "val": 32\n' \
            '}'
        script = "DECLARE $s AS Utf8; "\
                 "DECLARE $val AS Uint64; "\
                 "SELECT $s AS s, $val AS val; "
        self.write_data(param_data)
        output = self.execute_ydb_cli_command_with_db(command + ["-s", script], self.get_stdin())
        self.close_stdin()
        return self.canonical_result(output)

    def text_data(self, command):
        param_data = 'Line1\n' \
            'Line2\n' \
            'Line3\n'
        script = "DECLARE $s AS Utf8; " \
                 "SELECT $s AS s; "
        self.write_data(param_data)
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script, "--stdin-format", "raw", "--stdin-par", "s"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output)

    def unnamed_json(self, command):
        param_data = "[1, 2, 3, 4]"
        script = "DECLARE $arr AS List<Uint64>; "\
                 "SELECT $arr AS arr; "
        self.write_data(param_data)
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script, "--stdin-par", "arr"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output)

    def mix_json_and_binary(self, command):
        param_data1 = 'Строка номер 1\n' \
            'String number 2\n' \
            'Строка номер 3'
        param_data2 = '{\n' \
            '   "date": "2044-08-21",\n' \
            '   "val": 32\n' \
            '}'
        script = "DECLARE $s AS String; " \
                 "DECLARE $date AS Date; " \
                 "DECLARE $val AS Uint64; " \
                 "SELECT $s AS s, $date AS date, $val AS val; "
        self.write_data(param_data1)
        self.write_data(param_data2, "params.json")
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script, "--stdin-par", "s", "--stdin-format", "raw", "--param-file", "params.json"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output)

    def different_sources(self, command):
        param_data1 = '{\n' \
            '   "s": "Строка utf-8"\n' \
            '}'
        param_data2 = '{\n' \
            '   "date": "2000-09-01"\n' \
            '}'
        script = "DECLARE $s AS Utf8; " \
                 "DECLARE $date AS Date; " \
                 "DECLARE $val AS Uint64; " \
                 "SELECT $s AS s, $date AS date, $val AS val; "
        self.write_data(param_data1)
        self.write_data(param_data2, "params.json")
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script, "--param-file", "params.json", "--param", "$val=100"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output)

    def framing_newline_delimited(self, command):
        param_data = '{"s": "Some text", "num": 1}\n' \
            '{"s": "Строка 1\\nСтрока2", "num": 2}\n' \
            '{"s": "Abacaba", "num": 3}\n'
        script = "DECLARE $s AS Utf8; " \
                 "DECLARE $num AS Uint64; " \
                 "SELECT $s AS s, $num AS num; "
        self.write_data(param_data)
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script, "--stdin-format", "newline-delimited"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output)

    def text_data_framing(self, command):
        param_data = 'Line1\n' \
            'Line2\n' \
            'Line3\n'
        script = "DECLARE $s AS Utf8; " \
                 "SELECT $s AS s; "
        self.write_data(param_data)
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script, "--stdin-format", "raw", "--stdin-par", "s", "--stdin-format", "newline-delimited"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output)

    def batching_full(self, command):
        param_data = 'Line1\n' \
            'Line2\n' \
            'Line3\n'
        script = "DECLARE $s AS List<Utf8>; " \
                 "SELECT $s AS s; "
        self.write_data(param_data)
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script, "--stdin-format", "raw", "--stdin-par", "s",
                       "--stdin-format", "newline-delimited", "--batch", "full"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output)

    def batching_adaptive(self, command):
        param_data = '{"s": "Line1", "id": 1}\n' \
            '{"s": "Line2", "id": 2}\n' \
            '{"s": "Line3", "id": 3}\n' \
            '{"s": "Line4", "id": 4}\n' \
            '{"s": "Line5", "id": 5}\n' \
            '{"s": "Line6", "id": 6}\n' \
            '{"s": "Line7", "id": 7}\n' \
            '{"s": "Line8", "id": 8}\n' \
            '{"s": "Line9", "id": 9}\n'
        script = "DECLARE $arr AS List<Struct<s:Utf8, id:Uint64>>; " \
                 "SELECT $arr AS arr; "
        self.write_data(param_data)
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script, "--stdin-par", "arr", "--stdin-format", "newline-delimited",
                       "--batch", "adaptive", "--batch-max-delay", "0", "--batch-limit", "3"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output)

    def ignore_excess_parameters(self, command):
        param_data = '{\n' \
            '   "a": 12,\n' \
            '   "b": 34' \
            '}'
        script = "DECLARE $a AS Uint64; " \
                 "SELECT $a AS a; "
        self.write_data(param_data)
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output)

    def test_simple_json(self):
        return self.simple_json(["scripting", "yql"])

    def test_text_data(self):
        return self.text_data(["scripting", "yql"])

    def test_unnamed_json(self):
        return self.unnamed_json(["scripting", "yql"])

    def test_mix_json_and_binary(self):
        return self.mix_json_and_binary(["scripting", "yql"])

    def test_different_sources(self):
        return self.different_sources(["scripting", "yql"])

    def test_framing_newline_delimited(self):
        return self.framing_newline_delimited(["scripting", "yql"])

    def test_text_data_framing(self):
        return self.text_data_framing(["scripting", "yql"])

    def test_batching_full(self):
        return self.batching_full(["scripting", "yql"])

    def test_batching_adaptive(self):
        return self.batching_adaptive(["scripting", "yql"])

    def test_ignore_excess_parameters(self):
        return self.ignore_excess_parameters(["scripting", "yql"])

    def test_stream_simple_json(self):
        return self.simple_json(["yql"])

    def test_stream_text_data(self):
        return self.text_data(["yql"])

    def test_stream_unnamed_json(self):
        return self.unnamed_json(["yql"])

    def test_stream_mix_json_and_binary(self):
        return self.mix_json_and_binary(["yql"])

    def test_stream_different_sources(self):
        return self.different_sources(["yql"])

    def test_stream_framing_newline_delimited(self):
        return self.framing_newline_delimited(["yql"])

    def test_stream_text_data_framing(self):
        return self.text_data_framing(["yql"])

    def test_stream_batching_full(self):
        return self.batching_full(["yql"])

    def test_stream_batching_adaptive(self):
        return self.batching_adaptive(["yql"])

    def test_stream_ignore_excess_parameters(self):
        return self.ignore_excess_parameters(["yql"])
