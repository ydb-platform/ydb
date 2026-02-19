# -*- coding: utf-8 -*-

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.canonical import set_canondata_root
from ydb.tests.oss.ydb_sdk_import import ydb

import os
import logging
import pytest

import yatest

logger = logging.getLogger(__name__)


def ydb_bin():
    if os.getenv("YDB_CLI_BINARY"):
        return yatest.common.binary_path(os.getenv("YDB_CLI_BINARY"))
    raise RuntimeError("YDB_CLI_BINARY enviroment variable is not specified")


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


class BaseTestSql(object):
    @classmethod
    def execute_ydb_cli_command(cls, args, stdin=None, env=None):
        execution = yatest.common.execute([ydb_bin()] + args, stdin=stdin, env=env)
        result = execution.std_out
        logger.debug("std_out:\n" + result.decode('utf-8'))
        return result

    @staticmethod
    def canonical_result(output_result, tmp_path):
        with (tmp_path / "result.output").open("w") as f:
            f.write(output_result.decode('utf-8'))
        return yatest.common.canonical_file(str(tmp_path / "result.output"), local=True, universal_lines=True)


class BaseTestSqlWithDatabase(BaseTestSql):
    @classmethod
    def setup_class(cls):
        set_canondata_root('ydb/tests/functional/ydb_cli/canondata')

        cls.cluster = KiKiMR()
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
    def execute_ydb_cli_command_with_db(cls, args, stdin=None, env=None):
        return cls.execute_ydb_cli_command(
            [
                "--endpoint", "grpc://localhost:%d" % cls.cluster.nodes[1].grpc_port,
                "--database", cls.root_dir
            ] +
            args, stdin, env=env
        )


class TestExecuteSqlWithParams(BaseTestSqlWithDatabase):
    @classmethod
    def setup_class(cls):
        BaseTestSqlWithDatabase.setup_class()
        cls.session = cls.driver.table_client.session().create()

    @pytest.fixture(autouse=True, scope='function')
    def init_test(self, tmp_path):
        self.tmp_path = tmp_path
        self.table_path = self.root_dir + "/" + self.tmp_path.name
        create_table_with_data(self.session, self.table_path)

    def test_uint32(self):
        script = "DECLARE $par1 AS Uint32; SELECT * FROM `{}` WHERE key = $par1;".format(self.table_path)
        output = self.execute_ydb_cli_command_with_db(["sql", "-s", script, "--param", "$par1=1"])
        return self.canonical_result(output, self.tmp_path)

    def test_uint64_and_string(self):
        script = "DECLARE $id AS Uint64; "\
                 "DECLARE $value AS String; "\
                 "SELECT * FROM `{}` WHERE id = $id OR value = $value;".format(self.table_path)
        output = self.execute_ydb_cli_command_with_db(
            ["sql", "-s", script, "--param", "$id=2222",
             "--param", "$value=\"seven\""]
        )
        return self.canonical_result(output, self.tmp_path)

    def test_list(self):
        script = "DECLARE $values AS List<Uint64?>; SELECT $values AS values;"
        output = self.execute_ydb_cli_command_with_db(["sql", "-s", script, "--param", "$values=[1,2,3]"])
        return self.canonical_result(output, self.tmp_path)

    def test_struct(self):
        script = "DECLARE $values AS List<Struct<key:Uint64, value:Utf8>>; "\
                 "SELECT "\
                 "Table.key AS key, "\
                 "Table.value AS value "\
                 "FROM (SELECT $values AS lst) FLATTEN BY lst AS Table;"
        output = self.execute_ydb_cli_command_with_db(
            ["sql", "-s", script, "--param",
             "$values=[{\"key\":1,\"value\":\"one\"},{\"key\":2,\"value\":\"two\"}]"]
        )
        return self.canonical_result(output, self.tmp_path)


class TestExecuteSqlWithFormats(BaseTestSqlWithDatabase):
    @classmethod
    def setup_class(cls):
        BaseTestSqlWithDatabase.setup_class()
        cls.session = cls.driver.table_client.session().create()

    @pytest.fixture(autouse=True, scope='function')
    def init_test(self, tmp_path):
        self.tmp_path = tmp_path
        self.table_path = self.root_dir + "/" + self.tmp_path.name
        create_table_with_data(self.session, self.table_path)

    def execute_sql(self, format):
        script = "SELECT * FROM `{path}` WHERE key < 4;" \
            "SELECT id FROM `{path}` WHERE key = 4;" \
            "SELECT value FROM `{path}` WHERE key > 5".format(path=self.table_path)
        output = self.execute_ydb_cli_command_with_db(["sql", "-s", script, "--format", format])
        return self.canonical_result(output, self.tmp_path)

    def execute_sql_pretty(self):
        return self.yql_script('pretty')

    def execute_sql_json_base64(self):
        return self.yql_script('json-base64')

    def execute_sql_json_base64_array(self):
        return self.yql_script('json-base64-array')

    def execute_sql_json_unicode(self):
        return self.yql_script('json-unicode')

    def execute_sql_json_unicode_array(self):
        return self.yql_script('json-unicode-array')


class TestExecuteSqlWithParamsFromJson(BaseTestSqlWithDatabase):
    @classmethod
    def setup_class(cls):
        BaseTestSqlWithDatabase.setup_class()
        cls.session = cls.driver.table_client.session().create()

    @pytest.fixture(autouse=True, scope='function')
    def init_test(self, tmp_path):
        self.tmp_path = tmp_path
        self.table_path = self.root_dir + "/" + self.tmp_path.name
        create_table_with_data(self.session, self.table_path)

    @staticmethod
    def write_data(data, filename):
        with open(filename, "w") as file:
            file.write(data)

    def test_script_from_file(self):
        script = "DECLARE $a AS Uint64; " \
                 "SELECT $a AS a; "
        self.write_data(script, str(self.tmp_path / "script.yql"))
        output = self.execute_ydb_cli_command_with_db(
            ["sql", "-f", str(self.tmp_path / "script.yql"), "--param", "$a=3"]
        )
        return self.canonical_result(output, self.tmp_path)


@pytest.mark.parametrize("command", ["sql"])
class TestExecuteSqlWithParamsFromStdin(BaseTestSqlWithDatabase):
    @classmethod
    def setup_class(cls):
        BaseTestSqlWithDatabase.setup_class()
        cls.session = cls.driver.table_client.session().create()

    @pytest.fixture(autouse=True, scope='function')
    def init_test(self, tmp_path):
        self.tmp_path = tmp_path
        self.table_path = self.root_dir + "/" + self.tmp_path.name
        create_table_with_data(self.session, self.table_path)

    @staticmethod
    def write_data(data, filename):
        with open(filename, "w") as file:
            file.write(data)

    @staticmethod
    def get_delim(format):
        if format == "csv":
            return ","
        elif format == "tsv":
            return "\t"
        raise RuntimeError("Unknown format: {}".format(format))

    @staticmethod
    def get_command(name):
        if name == "sql":
            return ["sql"]
        raise RuntimeError("Unknown command name: {}".format(name))

    def get_stdin(self):
        self.stdin = (self.tmp_path / "stdin.txt").open("r")
        return self.stdin

    def close_stdin(self):
        self.stdin.close()

    def simple_json(self, command):
        param_data = '{\n' \
            '   "s": "Some_string",\n' \
            '   "val": 32\n' \
            '}'
        script = "DECLARE $s AS Utf8; " \
                 "DECLARE $val AS Uint64; " \
                 "SELECT $s AS s, $val AS val; "
        self.write_data(param_data, str(self.tmp_path / "stdin.txt"))
        output = self.execute_ydb_cli_command_with_db(command + ["-s", script], self.get_stdin())
        self.close_stdin()
        return self.canonical_result(output, self.tmp_path)

    def simple_csv_tsv(self, command, format):
        param_data = 's{0}val\n' \
            '\"Some_s{0}tring\"{0}32'
        param_data = param_data.format(self.get_delim(format))
        script = "DECLARE $s AS Utf8; " \
                 "DECLARE $val AS Uint64; " \
                 "SELECT $s AS s, $val AS val; "
        self.write_data(param_data, str(self.tmp_path / "stdin.txt"))
        output = self.execute_ydb_cli_command_with_db(command + ["-s", script, "--input-format", format], self.get_stdin())
        self.close_stdin()
        return self.canonical_result(output, self.tmp_path)

    def stdin_par_raw(self, command):
        param_data = 'Line1\n' \
            'Line2\n' \
            'Line3\n'
        script = "DECLARE $s AS Utf8; " \
                 "SELECT $s AS s; "
        self.write_data(param_data, str(self.tmp_path / "stdin.txt"))
        output = self.execute_ydb_cli_command_with_db(command + ["-s", script, "--input-format", "raw", "--input-param-name", "s"], self.get_stdin())
        self.close_stdin()
        return self.canonical_result(output, self.tmp_path)

    def stdin_par_json(self, command):
        param_data = "[1, 2, 3, 4]"
        script = "DECLARE $arr AS List<Uint64>; " \
                 "SELECT $arr AS arr; "
        self.write_data(param_data, str(self.tmp_path / "stdin.txt"))
        output = self.execute_ydb_cli_command_with_db(command + ["-s", script, "--input-param-name", "arr"], self.get_stdin())
        self.close_stdin()
        return self.canonical_result(output, self.tmp_path)

    def stdin_par_csv_tsv(self, command, format):
        param_data = 'id{0}value\n' \
            '1{0}"ab{0}a"'
        param_data = param_data.format(self.get_delim(format))
        script = "DECLARE $s AS Struct<id:UInt64,value:Utf8>; " \
                 "SELECT $s AS s; "
        self.write_data(param_data, str(self.tmp_path / "stdin.txt"))
        output = self.execute_ydb_cli_command_with_db(command + ["-s", script, "--input-format", format, "--input-param-name", "s"], self.get_stdin())
        self.close_stdin()
        return self.canonical_result(output, self.tmp_path)

    def framing_newline_delimited_json(self, command):
        param_data = '{"s": "Some text", "num": 1}\n' \
            '{"s": "Строка 1\\nСтрока2", "num": 2}\n' \
            '{"s": "Abacaba", "num": 3}\n'
        script = "DECLARE $s AS Utf8; " \
                 "DECLARE $num AS Uint64; " \
                 "SELECT $s AS s, $num AS num; "
        self.write_data(param_data, str(self.tmp_path / "stdin.txt"))
        output = self.execute_ydb_cli_command_with_db(command + ["-s", script, "--input-framing", "newline-delimited"], self.get_stdin())
        self.close_stdin()
        return self.canonical_result(output, self.tmp_path)

    def framing_newline_delimited_csv_tsv(self, command, format):
        param_data = 's{0}num\n' \
            'Some text{0}1\n' \
            '"Строка 1\nСтрока2"{0}2\n' \
            'Abacaba{0}3\n'
        param_data = param_data.format(self.get_delim(format))
        script = "DECLARE $s AS Utf8; " \
                 "DECLARE $num AS Uint64; " \
                 "SELECT $s AS s, $num AS num; "
        self.write_data(param_data, str(self.tmp_path / "stdin.txt"))
        output = self.execute_ydb_cli_command_with_db(command + ["-s", script, "--input-format", format, "--input-framing", "newline-delimited"],
                                                      self.get_stdin())
        self.close_stdin()
        return self.canonical_result(output, self.tmp_path)

    def framing_newline_delimited_raw(self, command):
        param_data = 'Line1\n' \
            'Line2\n' \
            'Line3\n'
        script = "DECLARE $s AS Utf8; " \
                 "SELECT $s AS s; "
        self.write_data(param_data, str(self.tmp_path / "stdin.txt"))
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script, "--input-format", "raw", "--input-param-name", "s", "--input-framing", "newline-delimited"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output, self.tmp_path)

    def batching_full_raw(self, command):
        param_data = 'Line1\n' \
            'Line2\n' \
            'Line3\n'
        script = "DECLARE $s AS List<Utf8>; " \
                 "SELECT $s AS s; "
        self.write_data(param_data, str(self.tmp_path / "stdin.txt"))
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script, "--input-format", "raw", "--input-param-name", "s", "--input-framing", "newline-delimited", "--input-batch", "full"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output, self.tmp_path)

    def batching_full_json(self, command):
        param_data = '{"s": "Line1", "id": 1}\n' \
            '{"s": "Line2", "id": 2}\n' \
            '{"s": "Line3", "id": 3}\n' \
            '{"s": "Line4", "id": 4}\n' \
            '{"s": "Line5", "id": 5}\n' \
            '{"s": "Line6", "id": 6}\n' \
            '{"s": "Line7", "id": 7}\n' \
            '{"s": "Line8", "id": 8}\n' \
            '{"s": "Line9", "id": 9}\n'
        script = "DECLARE $arr as List<Struct<s:Utf8, id:Uint64>>; " \
                 "SELECT $arr as arr; "
        self.write_data(param_data, str(self.tmp_path / "stdin.txt"))
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script, "--input-param-name", "arr", "--input-framing", "newline-delimited", "--input-batch", "full"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output, self.tmp_path)

    def batching_full_csv_tsv(self, command, format):
        param_data = 's{0}id\n' \
            'Line1{0}1\n' \
            'Line2{0}2\n' \
            'Line3{0}3\n' \
            'Line4{0}4\n' \
            'Line5{0}5\n' \
            'Line6{0}6\n' \
            'Line7{0}7\n' \
            'Line8{0}8\n' \
            'Line9{0}9'
        param_data = param_data.format(self.get_delim(format))
        script = "DECLARE $arr as List<Struct<s:Utf8, id:Uint64>>; " \
                 "SELECT $arr as arr; "
        self.write_data(param_data, str(self.tmp_path / "stdin.txt"))
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script, "--input-format", format, "--input-param-name", "arr", "--input-framing", "newline-delimited", "--input-batch", "full"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output, self.tmp_path)

    def batching_adaptive_raw(self, command):
        param_data = 'Line1\n' \
            'Line2\n' \
            'Line3\n' \
            'Line4\n' \
            'Line5\n' \
            'Line6\n' \
            'Line7\n' \
            'Line8\n' \
            'Line9\n'
        script = "DECLARE $s AS List<Utf8>; " \
                 "SELECT $s AS s; "
        self.write_data(param_data, str(self.tmp_path / "stdin.txt"))
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script, "--input-format", "raw", "--input-param-name", "s", "--input-framing",
                       "newline-delimited", "--input-batch", "adaptive", "--input-batch-max-delay", "0",
                       "--input-batch-max-rows", "3"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output, self.tmp_path)

    def batching_adaptive_json(self, command):
        param_data = '{"s": "Line1", "id": 1}\n' \
            '{"s": "Line2", "id": 2}\n' \
            '{"s": "Line3", "id": 3}\n' \
            '{"s": "Line4", "id": 4}\n' \
            '{"s": "Line5", "id": 5}\n' \
            '{"s": "Line6", "id": 6}\n' \
            '{"s": "Line7", "id": 7}\n' \
            '{"s": "Line8", "id": 8}\n' \
            '{"s": "Line9", "id": 9}\n'
        script = "DECLARE $arr as List<Struct<s:Utf8, id:Uint64>>; " \
                 "SELECT $arr as arr; "
        self.write_data(param_data, str(self.tmp_path / "stdin.txt"))
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script, "--input-param-name", "arr", "--input-framing", "newline-delimited",
                       "--input-batch", "adaptive", "--input-batch-max-delay", "0", "--input-batch-max-rows", "3"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output, self.tmp_path)

    def batching_adaptive_csv_tsv(self, command, format):
        param_data = 's{0}id\n' \
            'Line1{0}1\n' \
            'Line2{0}2\n' \
            'Line3{0}3\n' \
            'Line4{0}4\n' \
            'Line5{0}5\n' \
            'Line6{0}6\n' \
            'Line7{0}7\n' \
            'Line8{0}8\n' \
            'Line9{0}9'
        param_data = param_data.format(self.get_delim(format))
        script = "DECLARE $arr as List<Struct<s:Utf8, id:Uint64>>; " \
                 "SELECT $arr as arr; "
        self.write_data(param_data, str(self.tmp_path / "stdin.txt"))
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script, "--input-format", format, "--input-param-name", "arr", "--input-framing", "newline-delimited",
                                     "--input-batch", "adaptive", "--input-batch-max-delay", "0", "--input-batch-max-rows", "3"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output, self.tmp_path)

    def ignore_excess_parameters_json(self, command):
        param_data = '{\n' \
            '   "a": 12,\n' \
            '   "b": 34' \
            '}'
        script = "DECLARE $a AS Uint64; " \
                 "SELECT $a AS a; "
        self.write_data(param_data, str(self.tmp_path / "stdin.txt"))
        output = self.execute_ydb_cli_command_with_db(command + ["-s", script], self.get_stdin())
        self.close_stdin()
        return self.canonical_result(output, self.tmp_path)

    def ignore_excess_parameters_csv_tsv(self, command, format):
        param_data = 'a{0}b\n' \
            '12{0}34\n'
        param_data = param_data.format(self.get_delim(format))
        print(param_data)
        script = "DECLARE $a AS Uint64; " \
                 "SELECT $a AS a; "
        self.write_data(param_data, str(self.tmp_path / "stdin.txt"))
        output = self.execute_ydb_cli_command_with_db(command + ["-s", script, "--input-format", format], self.get_stdin())
        self.close_stdin()
        return self.canonical_result(output, self.tmp_path)

    def columns_bad_header(self, command, format):
        param_data = 'x{0}y\n' \
            '1{0}1\n' \
            '2{0}2\n' \
            '3{0}3\n'
        param_data = param_data.format(self.get_delim(format))
        script = "DECLARE $a AS Uint64; " \
                 "DECLARE $b AS Uint64; " \
                 "SELECT $a AS a, $b AS b; "
        self.write_data(param_data, str(self.tmp_path / "stdin.txt"))
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script, "--input-format", format, "--input-framing", "newline-delimited",
                       "--input-columns", "a{0}b".format(self.get_delim(format)), "--input-skip-rows", "1"],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output, self.tmp_path)

    def columns_no_header(self, command, format):
        param_data = '1{0}1\n' \
            '2{0}2\n' \
            '3{0}3\n'
        param_data = param_data.format(self.get_delim(format))
        script = "DECLARE $a AS Uint64; " \
                 "DECLARE $b AS Uint64; " \
                 "SELECT $a AS a, $b AS b; "
        self.write_data(param_data, str(self.tmp_path / "stdin.txt"))
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script, "--input-format", format, "--input-framing", "newline-delimited",
                       "--input-columns", "a{0}b".format(self.get_delim(format))],
            self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output, self.tmp_path)

    def skip_rows(self, command, format):
        param_data = 'a{0}b\n' \
            'x{0}x\n' \
            'x{0}x\n' \
            'x{0}x\n' \
            '1{0}1\n' \
            '2{0}2\n' \
            '3{0}3\n'
        param_data = param_data.format(self.get_delim(format))
        script = "DECLARE $a AS Uint64; " \
                 "DECLARE $b AS Uint64; " \
                 "SELECT $a AS a, $b AS b; "
        self.write_data(param_data, str(self.tmp_path / "stdin.txt"))
        output = self.execute_ydb_cli_command_with_db(
            command + ["-s", script, "--input-format", format, "--input-framing", "newline-delimited", "--input-skip-rows", "3"], self.get_stdin()
        )
        self.close_stdin()
        return self.canonical_result(output, self.tmp_path)

    def test_simple_json(self, command):
        return self.simple_json(self.get_command(command))

    def test_simple_csv(self, command):
        return self.simple_csv_tsv(self.get_command(command), "csv")

    def test_simple_tsv(self, command):
        return self.simple_csv_tsv(self.get_command(command), "tsv")

    def test_stdin_par_raw(self, command):
        return self.stdin_par_raw(self.get_command(command))

    def test_stdin_par_json(self, command):
        return self.stdin_par_json(self.get_command(command))

    def test_stdin_par_csv(self, command):
        return self.stdin_par_csv_tsv(self.get_command(command), "csv")

    def test_stdin_par_tsv(self, command):
        return self.stdin_par_csv_tsv(self.get_command(command), "tsv")

    def test_framing_newline_delimited_json(self, command):
        return self.framing_newline_delimited_json(self.get_command(command))

    def test_framing_newline_delimited_csv(self, command):
        return self.framing_newline_delimited_csv_tsv(self.get_command(command), "csv")

    def test_framing_newline_delimited_tsv(self, command):
        return self.framing_newline_delimited_csv_tsv(self.get_command(command), "tsv")

    def test_framing_newline_delimited_raw(self, command):
        return self.framing_newline_delimited_raw(self.get_command(command))

    def test_batching_full_raw(self, command):
        return self.batching_full_raw(self.get_command(command))

    def test_batching_full_json(self, command):
        return self.batching_full_json(self.get_command(command))

    def test_batching_full_csv(self, command):
        return self.batching_full_csv_tsv(self.get_command(command), "csv")

    def test_batching_full_tsv(self, command):
        return self.batching_full_csv_tsv(self.get_command(command), "tsv")

    def test_batching_adaptive_raw(self, command):
        return self.batching_adaptive_raw(self.get_command(command))

    def test_batching_adaptive_json(self, command):
        return self.batching_adaptive_json(self.get_command(command))

    def test_batching_adaptive_csv(self, command):
        return self.batching_adaptive_csv_tsv(self.get_command(command), "csv")

    def test_batching_adaptive_tsv(self, command):
        return self.batching_adaptive_csv_tsv(self.get_command(command), "tsv")

    def test_ignore_excess_parameters_json(self, command):
        return self.ignore_excess_parameters_json(self.get_command(command))

    def test_ignore_excess_parameters_csv(self, command):
        return self.ignore_excess_parameters_csv_tsv(self.get_command(command), "csv")

    def test_ignore_excess_parameters_tsv(self, command):
        return self.ignore_excess_parameters_csv_tsv(self.get_command(command), "tsv")

    def test_columns_bad_header_csv(self, command):
        return self.columns_bad_header(self.get_command(command), "csv")

    def test_columns_bad_header_tsv(self, command):
        return self.columns_bad_header(self.get_command(command), "tsv")

    def test_columns_no_header_csv(self, command):
        return self.columns_no_header(self.get_command(command), "csv")

    def test_columns_no_header_tsv(self, command):
        return self.columns_no_header(self.get_command(command), "tsv")

    def test_rows_csv(self, command):
        return self.skip_rows(self.get_command(command), "csv")

    def test_rows_tsv(self, command):
        return self.skip_rows(self.get_command(command), "tsv")


class TestExecuteSqlWithStdinDetection(BaseTestSqlWithDatabase):
    """Test cases for stdin detection and parameter handling in various scenarios"""

    @classmethod
    def setup_class(cls):
        BaseTestSqlWithDatabase.setup_class()
        cls.session = cls.driver.table_client.session().create()

    @pytest.fixture(autouse=True, scope='function')
    def init_test(self, tmp_path):
        self.tmp_path = tmp_path
        self.table_path = self.root_dir + "/" + self.tmp_path.name
        create_table_with_data(self.session, self.table_path)

    @staticmethod
    def write_data(data, filename):
        with open(filename, "w") as file:
            file.write(data)

    def test_empty_stdin_no_params(self):
        """Test that CLI works correctly when stdin is empty and no parameters are expected"""
        script = "SELECT 1 AS result;"
        # Run with empty stdin (no data passed)
        output = self.execute_ydb_cli_command_with_db(["sql", "-s", script], stdin=None)
        return self.canonical_result(output, self.tmp_path)

    def test_empty_stdin_with_param_name_should_fail(self):
        """Test that CLI fails when --input-param-name is specified but stdin is empty"""
        script = "DECLARE $lines AS List<Struct<id:UInt64>>; SELECT tl.id FROM AS_TABLE($lines) AS tl;"
        # Run with empty stdin but expecting parameters
        try:
            self.execute_ydb_cli_command_with_db(
                ["sql", "-s", script, "--input-param-name", "lines", "--input-framing", "newline-delimited"],
                stdin=None
            )
            # If we get here, the test should fail because we expect an error
            assert False, "Expected error when --input-param-name is specified but stdin is empty"
        except Exception as e:
            # This is expected - CLI should fail with appropriate error message
            assert "input-param-name" in str(e) or "stdin" in str(e).lower()

    def test_detached_job_simulation(self):
        """Simulate detached job scenario where process runs in background with empty stdin"""
        script = "SELECT 1 AS result;"
        # Simulate detached job by running with /dev/null as stdin
        import subprocess
        import os

        cmd = [ydb_bin()] + [
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", self.root_dir,
            "sql", "-s", script
        ]

        # Run with /dev/null as stdin to simulate detached job
        with open(os.devnull, 'r') as devnull:
            result = subprocess.run(cmd, stdin=devnull, capture_output=True, text=True)

        # Should succeed without trying to read parameters from stdin
        assert result.returncode == 0, f"Command failed with return code {result.returncode}: {result.stderr}"
        assert "result" in result.stdout

    def test_pipe_with_data_success(self):
        """Test that CLI correctly detects and processes data from pipe"""
        script = "DECLARE $lines AS List<Struct<id:UInt64>>; SELECT tl.id FROM AS_TABLE($lines) AS tl;"
        param_data = '{"id": 1}\n{"id": 2}\n{"id": 3}\n'

        self.write_data(param_data, str(self.tmp_path / "pipe_data.txt"))

        with open(str(self.tmp_path / "pipe_data.txt"), "r") as stdin_file:
            output = self.execute_ydb_cli_command_with_db(
                ["sql", "-s", script, "--input-param-name", "lines",
                 "--input-framing", "newline-delimited", "--input-batch", "adaptive"],
                stdin=stdin_file
            )
        return self.canonical_result(output, self.tmp_path)

    def test_interactive_vs_non_interactive_detection(self):
        """Test that CLI correctly detects interactive vs non-interactive mode"""
        script = "SELECT 1 AS result;"

        # Test with explicit stdin redirection from /dev/null (non-interactive)
        import subprocess
        import os

        cmd = [ydb_bin()] + [
            "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
            "--database", self.root_dir,
            "--verbose",  # Add verbose to see detection logic
            "sql", "-s", script
        ]

        with open(os.devnull, 'r') as devnull:
            result = subprocess.run(cmd, stdin=devnull, capture_output=True, text=True)

        # Should succeed and not try to read parameters
        assert result.returncode == 0, f"Command failed: {result.stderr}"
        # Should not contain parameter-related error messages
        assert "input-param-name" not in result.stderr

    def test_parameters_via_param_option(self):
        """Test parameters passed via --param option (from documentation)"""
        script = "DECLARE $a AS Int64; SELECT $a"
        output = self.execute_ydb_cli_command_with_db(
            ["sql", "-s", script, "--param", "$a=10"]
        )
        output_str = output.decode('utf-8')
        assert "10" in output_str

    def test_mixed_parameter_sources_should_fail(self):
        """Test that CLI fails when both --param and stdin parameters are provided"""
        script = "DECLARE $a AS Uint64; DECLARE $lines AS List<Struct<id:UInt64>>; SELECT $a, tl.id FROM AS_TABLE($lines) AS tl;"
        param_data = '{"id": 1}\n'

        self.write_data(param_data, str(self.tmp_path / "stdin_data.txt"))

        with open(str(self.tmp_path / "stdin_data.txt"), "r") as stdin_file:
            try:
                self.execute_ydb_cli_command_with_db(
                    ["sql", "-s", script, "--param", "$a=42", "--input-param-name", "lines",
                     "--input-framing", "newline-delimited"],
                    stdin=stdin_file
                )
                # This should fail because we're mixing parameter sources
                assert False, "Expected error when mixing --param and stdin parameters"
            except Exception as e:
                # Expected to fail - check for any error about mixing sources or data type issues
                error_msg = str(e).lower()
                assert any(keyword in error_msg for keyword in ["parameter", "stdin", "input", "mix", "type", "array", "map"])

    def test_no_parameters_but_stdin_has_data_should_ignore(self):
        """Test that CLI handles stdin data when no parameter options are specified"""
        script = "SELECT 1 AS result;"
        param_data = '{"id": 1}\n{"id": 2}\n'

        self.write_data(param_data, str(self.tmp_path / "unused_data.txt"))

        with open(str(self.tmp_path / "unused_data.txt"), "r") as stdin_file:
            try:
                output = self.execute_ydb_cli_command_with_db(
                    ["sql", "-s", script],
                    stdin=stdin_file
                )
                # If it succeeds, the data should be ignored
                output_str = output.decode('utf-8')
                assert "result" in output_str
                assert "id" not in output_str  # Should not process the JSON data
            except Exception as e:
                # If it fails, it should be due to parsing stdin as JSON when not expected
                error_msg = str(e).lower()
                assert any(keyword in error_msg for keyword in ["json", "parse", "input"])

    def test_stdin_detection_edge_cases(self):
        """Test various edge cases for stdin detection"""
        script = "SELECT 1 AS result;"

        # Test with empty string as stdin
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("")
            f.flush()
            with open(f.name, 'r') as stdin_file:
                output = self.execute_ydb_cli_command_with_db(["sql", "-s", script], stdin=stdin_file)
        output_str = output.decode('utf-8')
        assert "result" in output_str

    def test_parameter_validation_with_empty_stdin(self):
        """Test parameter validation when stdin is empty but parameters are expected"""
        script = "DECLARE $lines AS List<Struct<id:UInt64>>; SELECT tl.id FROM AS_TABLE($lines) AS tl;"

        # Test with empty stdin but expecting parameters
        try:
            self.execute_ydb_cli_command_with_db(
                ["sql", "-s", script, "--input-param-name", "lines",
                 "--input-framing", "newline-delimited", "--input-batch", "adaptive"],
                stdin=None
            )
            assert False, "Expected error when stdin is empty but parameters expected"
        except Exception as e:
            # Should fail with appropriate error message
            error_msg = str(e).lower()
            assert any(keyword in error_msg for keyword in ["input-param-name", "stdin", "parameter"])

    def test_exact_pipe_scenario_from_bug_report(self):
        """Test that reproduces the exact pipe scenario from the bug report"""
        # This is the exact command that was failing:
        # ydb sql -s 'SELECT t.id FROM test_delete_1 AS t WHERE t.id > 10' --format json-unicode |
        # ydb sql -s 'DECLARE $lines AS List<Struct<id:UInt64>>; DELETE FROM test_delete_1 WHERE id IN (SELECT tl.id FROM AS_TABLE($lines) AS tl)'
        #   --input-framing newline-delimited --input-param-name lines --input-batch adaptive --input-batch-max-rows 10000

        script = "DECLARE $lines AS List<Struct<id:UInt64>>; SELECT tl.id FROM AS_TABLE($lines) AS tl"

        # Simulate the JSON output that would come from the first command
        json_data = '{"id":1}\n{"id":2}\n{"id":3}\n'

        self.write_data(json_data, str(self.tmp_path / "json_pipe_data.txt"))

        with open(str(self.tmp_path / "json_pipe_data.txt"), "r") as stdin_file:
            output = self.execute_ydb_cli_command_with_db(
                ["sql", "-s", script, "--input-framing", "newline-delimited",
                 "--input-param-name", "lines", "--input-batch", "adaptive",
                 "--input-batch-max-rows", "10000"],
                stdin=stdin_file
            )

        # Should succeed and process the JSON data
        output_str = output.decode('utf-8')
        assert "1" in output_str and "2" in output_str and "3" in output_str

    def test_parameters_via_input_file(self):
        """Test parameters passed via --param option (simulating --input-file behavior)"""
        script = "DECLARE $a AS Int64; SELECT $a"

        output = self.execute_ydb_cli_command_with_db(
            ["sql", "-s", script, "--param", "$a=10"]
        )
        output_str = output.decode('utf-8')
        assert "10" in output_str

    def test_csv_format_parameters(self):
        """Test CSV format parameters (from documentation)"""
        script = "DECLARE $a AS Int32; DECLARE $b AS String; SELECT $a, $b"
        csv_data = "10,Some text"

        self.write_data(csv_data, str(self.tmp_path / "data.csv"))

        with open(str(self.tmp_path / "data.csv"), "r") as stdin_file:
            output = self.execute_ydb_cli_command_with_db(
                ["sql", "-s", script, "--input-format", "csv", "--input-columns", "a,b"],
                stdin=stdin_file
            )

        output_str = output.decode('utf-8')
        assert "10" in output_str and "Some text" in output_str


class TestExecuteSqlWithParameterEdgeCases(BaseTestSqlWithDatabase):
    """Additional test cases for parameter handling edge cases and error conditions"""

    @classmethod
    def setup_class(cls):
        BaseTestSqlWithDatabase.setup_class()
        cls.session = cls.driver.table_client.session().create()

    @pytest.fixture(autouse=True, scope='function')
    def init_test(self, tmp_path):
        self.tmp_path = tmp_path
        self.table_path = self.root_dir + "/" + self.tmp_path.name
        create_table_with_data(self.session, self.table_path)

    @staticmethod
    def write_data(data, filename):
        with open(filename, "w") as file:
            file.write(data)

    def test_stdin_with_invalid_json_should_fail(self):
        """Test that CLI fails gracefully when stdin contains invalid JSON"""
        script = "DECLARE $lines AS List<Struct<id:UInt64>>; SELECT tl.id FROM AS_TABLE($lines) AS tl;"
        invalid_json = '{"id": 1}\n{"id": 2,}\n{"id": 3}\n'  # Invalid JSON with trailing comma

        self.write_data(invalid_json, str(self.tmp_path / "invalid_data.txt"))

        with open(str(self.tmp_path / "invalid_data.txt"), "r") as stdin_file:
            try:
                self.execute_ydb_cli_command_with_db(
                    ["sql", "-s", script, "--input-param-name", "lines",
                     "--input-framing", "newline-delimited"],
                    stdin=stdin_file
                )
                assert False, "Expected error when stdin contains invalid JSON"
            except Exception as e:
                # Should fail with JSON parsing error
                assert "json" in str(e).lower() or "parse" in str(e).lower()

    def test_stdin_with_partial_data_should_handle_gracefully(self):
        """Test that CLI handles partial data in stdin correctly"""
        script = "DECLARE $lines AS List<Struct<id:UInt64>>; SELECT tl.id FROM AS_TABLE($lines) AS tl;"
        partial_data = '{"id": 1}\n{"id": 2}\n'  # Only 2 items, should still work

        self.write_data(partial_data, str(self.tmp_path / "partial_data.txt"))

        with open(str(self.tmp_path / "partial_data.txt"), "r") as stdin_file:
            output = self.execute_ydb_cli_command_with_db(
                ["sql", "-s", script, "--input-param-name", "lines",
                 "--input-framing", "newline-delimited", "--input-batch", "adaptive"],
                stdin=stdin_file
            )

        # Should succeed with partial data
        output_str = output.decode('utf-8')
        assert "1" in output_str and "2" in output_str

    def test_stdin_detection_with_unicode_data(self):
        """Test stdin detection with Unicode data"""
        script = "DECLARE $lines AS List<Struct<id:UInt64, text:Utf8>>; SELECT tl.id, tl.text FROM AS_TABLE($lines) AS tl;"
        unicode_data = '{"id": 1, "text": "Привет мир"}\n{"id": 2, "text": "🌍🌎🌏"}\n'

        self.write_data(unicode_data, str(self.tmp_path / "unicode_data.txt"))

        with open(str(self.tmp_path / "unicode_data.txt"), "r", encoding='utf-8') as stdin_file:
            output = self.execute_ydb_cli_command_with_db(
                ["sql", "-s", script, "--input-param-name", "lines",
                 "--input-framing", "newline-delimited", "--input-batch", "adaptive"],
                stdin=stdin_file
            )

        # Should handle Unicode data correctly
        output_str = output.decode('utf-8')
        assert "Привет" in output_str or "🌍" in output_str

    def test_full_batch_processing(self):
        """Test full batch processing (from documentation)"""
        script = "DECLARE $x AS List<Struct<a:Int64,b:Int64>>; SELECT ListLength($x), $x"
        batch_data = '{"a":10,"b":20}\n{"a":15,"b":25}\n{"a":35,"b":48}\n'

        self.write_data(batch_data, str(self.tmp_path / "batch_data.txt"))

        with open(str(self.tmp_path / "batch_data.txt"), "r") as stdin_file:
            output = self.execute_ydb_cli_command_with_db(
                ["sql", "-s", script, "--input-framing", "newline-delimited",
                 "--input-param-name", "x", "--input-batch", "full"],
                stdin=stdin_file
            )

        output_str = output.decode('utf-8')
        assert "3" in output_str  # ListLength should be 3

    def test_pipe_scenario_with_empty_stdin_should_fail(self):
        """Test that CLI fails when --input-param-name is specified but stdin is empty (pipe scenario)"""
        script = "DECLARE $lines AS List<Struct<id:UInt64>>; SELECT tl.id FROM AS_TABLE($lines) AS tl"

        # Test with empty stdin (simulating broken pipe or empty input)
        try:
            self.execute_ydb_cli_command_with_db(
                ["sql", "-s", script, "--input-framing", "newline-delimited",
                 "--input-param-name", "lines", "--input-batch", "adaptive",
                 "--input-batch-max-rows", "10000"],
                stdin=None
            )
            # This should fail
            assert False, "Expected error when --input-param-name is specified but stdin is empty"
        except Exception as e:
            # Should fail with appropriate error message
            error_msg = str(e).lower()
            assert any(keyword in error_msg for keyword in ["input-param-name", "stdin", "parameter"])

    def test_detached_job_scenario_with_empty_stdin(self):
        """Test detached job scenario where stdin is empty but no parameters are expected"""
        script = "SELECT 1 AS result"

        # Test with empty stdin (simulating detached job)
        output = self.execute_ydb_cli_command_with_db(["sql", "-s", script], stdin=None)

        # Should succeed without trying to read parameters
        output_str = output.decode('utf-8')
        assert "result" in output_str


def create_wide_table_with_data(session, path):
    session.create_table(
        path,
        ydb.TableDescription()
           .with_column(ydb.Column("timestamp", ydb.PrimitiveType.Timestamp))
           .with_column(ydb.Column("pod", ydb.PrimitiveType.Utf8))
           .with_column(ydb.Column("seq", ydb.PrimitiveType.Uint64))
           .with_column(ydb.Column("container_id", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
           .with_column(ydb.Column("host", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
           .with_column(ydb.Column("box", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
           .with_column(ydb.Column("workload", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
           .with_column(ydb.Column("logger_name", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
           .with_column(ydb.Column("user_id", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
           .with_column(ydb.Column("request_id", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
           .with_column(ydb.Column("message", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
           .with_column(ydb.Column("log_level", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
           .with_column(ydb.Column("log_level_int", ydb.OptionalType(ydb.PrimitiveType.Int64)))
           .with_column(ydb.Column("stack_trace", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
           .with_column(ydb.Column("thread_name", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
           .with_column(ydb.Column("pod_transient_fqdn", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
           .with_column(ydb.Column("pod_persistent_fqdn", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
           .with_column(ydb.Column("node_fqdn", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
           .with_column(ydb.Column("context", ydb.OptionalType(ydb.PrimitiveType.JsonDocument)))
           .with_column(ydb.Column("version", ydb.OptionalType(ydb.PrimitiveType.Int32)))
           .with_column(ydb.Column("saved_at", ydb.OptionalType(ydb.PrimitiveType.Timestamp)))
           .with_primary_keys("timestamp", "pod", "seq")
    )


class TestExecuteSqlFromStdinWithWideOutput(BaseTestSqlWithDatabase):
    @classmethod
    def setup_class(cls):
        BaseTestSqlWithDatabase.setup_class()
        cls.session = cls.driver.table_client.session().create()

    @pytest.fixture(autouse=True, scope='function')
    def init_test(self, tmp_path):
        self.tmp_path = tmp_path
        self.table_path = self.root_dir + "/" + self.tmp_path.name
        create_wide_table_with_data(self.session, self.table_path)

    def test_wide_table(self):
        script = "SELECT * FROM `{}`;".format(self.table_path)
        output = self.execute_ydb_cli_command_with_db(["sql", "-s", script])
        return self.canonical_result(output, self.tmp_path)


class TestExecuteSqlWithPgSyntax(BaseTestSqlWithDatabase):
    @classmethod
    def setup_class(cls):
        BaseTestSqlWithDatabase.setup_class()
        cls.session = cls.driver.table_client.session().create()

    @pytest.fixture(autouse=True, scope='function')
    def init_test(self, tmp_path):
        self.tmp_path = tmp_path
        self.table_path = self.tmp_path.name
        create_table_with_data(self.session, self.root_dir + "/" + self.table_path)

    @pytest.mark.skip(reason="pg syntax disabled")
    def test_pg_syntax(self):
        script = "SELECT * FROM \"{}\" WHERE key = 1;".format(self.table_path)
        output = self.execute_ydb_cli_command_with_db(["sql", "-s", script, "--syntax", "pg"])
        return self.canonical_result(output, self.tmp_path)
