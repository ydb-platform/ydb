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

    def test_skip_rows_csv(self, command):
        return self.skip_rows(self.get_command(command), "csv")

    def test_skip_rows_tsv(self, command):
        return self.skip_rows(self.get_command(command), "tsv")


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
