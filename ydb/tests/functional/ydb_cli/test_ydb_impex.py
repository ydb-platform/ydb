# -*- coding: utf-8 -*-
from ydb.tests.functional.ydb_cli.ydb_cli_helpers import BaseCliTestWithDatabase, ydb_bin

import pytest
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

import yatest

logger = logging.getLogger(__name__)

DATA = {}
DATA_ARRAY = {}
DATA_ARRAY_BAD_HEADER = {}
DATA_END_LINES = {}
DATA_EXCESS = {}

DATA["csv"] = """key,id,value
1,1111,"one"
2,2222,"two"
3,3333,"three"
5,5555,"five"
7,7777,"seven"
"""

DATA_EXCESS["csv"] = """key,id,value,excess
1,1111,"one",1
2,2222,"two",1
3,3333,"three",1
5,5555,"five",1
7,7777,"seven",1
"""

DATA_ARRAY["csv"] = ["""key,id,value
1,1111,"one"
2,2222,"two"
3,3333,"three"
5,5555,"five"
7,7777,"seven"
""", """key,id,value
11,1111,"one"
22,2222,"two"
33,3333,"three"
55,5555,"five"
77,7777,"seven"
""", """key,id,value
111,1111,"one"
222,2222,"two"
333,3333,"three"
555,5555,"five"
777,7777,"seven"
""", """key,id,value
1111,1111,"one"
2222,2222,"two"
3333,3333,"three"
5555,5555,"five"
7777,7777,"seven"
"""]

DATA_ARRAY_BAD_HEADER["csv"] = DATA_ARRAY["csv"]
DATA_ARRAY_BAD_HEADER["csv"][0].replace("key,id,value", 'a,b,c')

DATA_END_LINES["csv"] = DATA["csv"].replace("\n", ",\n")

DATA["tsv"] = DATA["csv"].replace(',', '\t')
DATA_EXCESS["tsv"] = DATA_EXCESS["csv"].replace(',', '\t')

DATA_ARRAY["tsv"] = list(map(lambda s: s.replace(',', '\t'), DATA_ARRAY["csv"]))
DATA_ARRAY_BAD_HEADER["tsv"] = DATA_ARRAY["tsv"]
DATA_ARRAY_BAD_HEADER["tsv"][0].replace("key\tid\tvalue", 'a\tb\tc')

DATA_END_LINES["tsv"] = DATA["tsv"].replace('\n', '\t\n')

DATA["json"] = """{"key":1,"id":1111,"value":"one"}
{"key":2,"id":2222,"value":"two"}
{"key":3,"id":3333,"value":"three"}
{"key":5,"id":5555,"value":"five"}
{"key":7,"id":7777,"value":"seven"}
"""

DATA_EXCESS["json"] = """{"key":1,"id":1111,"value":"one","excess":1}
{"key":2,"id":2222,"value":"two","excess":1}
{"key":3,"id":3333,"value":"three","excess":1}
{"key":5,"id":5555,"value":"five","excess":1}
{"key":7,"id":7777,"value":"seven","excess":1}
"""

DATA_ARRAY["json"] = ["""{"key":1,"id":1111,"value":"one"}
{"key":2,"id":2222,"value":"two"}
{"key":3,"id":3333,"value":"three"}
{"key":5,"id":5555,"value":"five"}
{"key":7,"id":7777,"value":"seven"}
""", """{"key":11,"id":1111,"value":"one"}
{"key":22,"id":2222,"value":"two"}
{"key":33,"id":3333,"value":"three"}
{"key":55,"id":5555,"value":"five"}
{"key":77,"id":7777,"value":"seven"}
""", """{"key":111,"id":1111,"value":"one"}
{"key":222,"id":2222,"value":"two"}
{"key":333,"id":3333,"value":"three"}
{"key":555,"id":5555,"value":"five"}
{"key":667,"id":7777,"value":"seven"}
""", """{"key":1111,"id":1111,"value":"one"}
{"key":2222,"id":2222,"value":"two"}
{"key":3333,"id":3333,"value":"three"}
{"key":5555,"id":5555,"value":"five"}
{"key":7777,"id":7777,"value":"seven"}
"""]

FILES_COUNT = 3
DATASET_SIZE = 100000

ARRAYS = [pa.array([1, 2, 3, 5, 7], type=pa.uint32()), pa.array([1111, 2222, 3333, 5555, 7777], type=pa.uint64()), pa.array(["one", "two", "three", "five", "seven"], type=pa.string())]
ARRAY_NAMES = ['key', 'id', 'value']
DATA_PARQUET = pa.Table.from_arrays(ARRAYS, names=ARRAY_NAMES)

ALL_PARAMS = [("csv", []), ("csv", ["--newline-delimited"]), ("tsv", []), ("tsv", ["--newline-delimited"]), ("json", [])]
ONLY_CSV_TSV_PARAMS = [("csv", []), ("csv", ["--newline-delimited"]), ("tsv", []), ("tsv", ["--newline-delimited"])]

BOM_UTF8 = b'\xEF\xBB\xBF'


def create_table(session, path, table_type):
    partition_by = ""
    if table_type == "column":
        partition_by = "PARTITION BY HASH(key)"
    query = \
        """
        CREATE TABLE `{}` (
            key Uint32 NOT NULL,
            id Uint64,
            value Utf8,
            PRIMARY KEY(key)
        )
        {}
        WITH (
            STORE = {}
        )
        """.format(path, partition_by, table_type)
    session.execute_scheme(query)


@pytest.mark.parametrize("table_type", ["row", "column"])
class TestImpex(BaseCliTestWithDatabase):

    @classmethod
    def setup_class(cls):
        super().setup_class()
        cls.session = cls.driver.table_client.session().create()

    def init_test(self, tmp_path, table_type, name):
        self.tmp_path = tmp_path
        self.table_type = table_type
        self.table_path = self.root_dir + "/" + name
        create_table(self.session, self.table_path, self.table_type)

    def write_array_to_files(self, arr, ftype):
        for i in range(len(arr)):
            with (self.tmp_path / "tempinput{}.{}".format(i, ftype)).open("w") as f:
                f.writelines(arr[i])

    @staticmethod
    def get_header_flag(ftype):
        if ftype == "csv" or ftype == "tsv":
            return ["--header"]
        return []

    @staticmethod
    def get_header(ftype):
        if ftype == "csv":
            return "key,id,value\n"
        if ftype == "tsv":
            return "key\tid\tvalue\n"
        return ""

    @staticmethod
    def get_row_in_format(ftype, key, id, value):
        if ftype == "csv":
            return '{},{},"{}"\n'.format(key, id, value)
        if ftype == "tsv":
            return '{}\t{}\t"{}"\n'.format(key, id, value)
        if ftype == "json":
            return '{' + '"key": {}, "id": {}, "value":"{}"'.format(key, id, value) + '}\n'
        raise RuntimeError("Not supported format used")

    def gen_dataset(self, rows, files, ftype):
        id_set = [10, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200]
        value_set = ["", "aaaaaaaaaa", "bbbbbbbbbb", "ccccccccc", "ddddddd", "eeeeeeeee", "fffffffff"]
        for i in range(files):
            with (self.tmp_path / "tempinput{}.{}".format(i, ftype)).open("w") as f:
                f.write(TestImpex.get_header(ftype))
                for key in range(i * rows, (i + 1) * rows):
                    f.write(TestImpex.get_row_in_format(ftype, key, id_set[key % len(id_set)], value_set[key % len(value_set)]))

    def write_data_to_tmp_file(self, path, add_bom, data):
        if add_bom:
            with path.open("wb") as f:
                f.write(BOM_UTF8 + data.encode('utf-8'))
        else:
            with path.open("w") as f:
                f.writelines(data)

    def run_import(self, ftype, data, additional_args=[], add_bom=False):
        path = self.tmp_path / "tempinput.{}".format(ftype)
        self.write_data_to_tmp_file(path, add_bom, data)
        self.execute_ydb_cli_command(["import", "file", ftype, "-p", self.table_path, "-i", str(path)] + self.get_header_flag(ftype) + additional_args)

    def run_import_from_stdin(self, ftype, data, additional_args=[], add_bom=False):
        path = self.tmp_path / "tempinput.{}".format(ftype)
        self.write_data_to_tmp_file(path, add_bom, data)
        with (path).open("r") as f:
            self.execute_ydb_cli_command(["import", "file", ftype, "-p", self.table_path] + self.get_header_flag(ftype) + additional_args, stdin=f)

    def run_import_multiple_files(self, ftype, files_count, additional_args=[]):
        args = ["import", "file", ftype, "-p", self.table_path] + self.get_header_flag(ftype) + additional_args
        for i in range(files_count):
            args.append(str(self.tmp_path / "tempinput{}.{}".format(i, ftype)))
        self.execute_ydb_cli_command(args)

    def run_import_multiple_files_and_stdin(self, ftype, files_count, additional_args=[]):
        args = ["import", "file", ftype, "-p", self.table_path] + self.get_header_flag(ftype) + additional_args
        for i in range(1, files_count):
            args.append(str(self.tmp_path / "tempinput{}.{}".format(i, ftype)))
        with (self.tmp_path / "tempinput0.{}".format(ftype)).open("r") as f:
            self.execute_ydb_cli_command(args, stdin=f)

    def run_import_parquet(self, data):
        path = self.tmp_path / "tempinput.parquet"
        with path.open("w"):
            pq.write_table(data, str(path), version="2.4")
        self.execute_ydb_cli_command(["import", "file", "parquet", "-p", self.table_path, "-i", str(path)])

    def run_export(self, format):
        if format == "json":
            format = "json-unicode"
        query = "SELECT `key`, `id`, `value` FROM `{}` ORDER BY `key`".format(self.table_path)
        output_file_name = str(self.tmp_path / "result.output")
        self.execute_ydb_cli_command(["table", "query", "execute", "-q", query, "-t", "scan", "--format", format], stdout=output_file_name)
        return yatest.common.canonical_file(output_file_name, local=True, universal_lines=True)

    def validate_gen_data(self):
        query = "SELECT count(*) FROM `{}`".format(self.table_path)
        output_file_name = str(self.tmp_path / "result.output")
        self.execute_ydb_cli_command(["table", "query", "execute", "-q", query, "-t", "scan"], stdout=output_file_name)
        return yatest.common.canonical_file(output_file_name, local=True, universal_lines=True)

    @pytest.mark.parametrize("ftype,additional_args", ALL_PARAMS)
    def test_simple(self, tmp_path, request, table_type, ftype, additional_args):
        self.init_test(tmp_path, table_type, request.node.name)
        self.run_import(ftype, DATA[ftype], additional_args)
        return self.run_export(ftype)

    @pytest.mark.parametrize("ftype,additional_args", ONLY_CSV_TSV_PARAMS)
    def test_delimeter_at_end_of_lines(self, tmp_path, request, table_type, ftype, additional_args):
        self.init_test(tmp_path, table_type, request.node.name)
        self.run_import(ftype, DATA_END_LINES[ftype], additional_args)
        return self.run_export(ftype)

    @pytest.mark.parametrize("ftype,additional_args", ALL_PARAMS)
    def test_excess_columns(self, tmp_path, request, table_type, ftype, additional_args):
        self.init_test(tmp_path, table_type, request.node.name)
        self.run_import(ftype, DATA_EXCESS[ftype], additional_args)
        return self.run_export(ftype)

    @pytest.mark.parametrize("ftype,additional_args", ALL_PARAMS)
    def test_stdin(self, tmp_path, request, table_type, ftype, additional_args):
        self.init_test(tmp_path, table_type, request.node.name)
        self.run_import_from_stdin(ftype, DATA[ftype], additional_args)
        return self.run_export(ftype)

    @pytest.mark.parametrize("ftype,additional_args", ALL_PARAMS)
    def test_multiple_files(self, tmp_path, request, table_type, ftype, additional_args):
        self.init_test(tmp_path, table_type, request.node.name)
        self.write_array_to_files(DATA_ARRAY[ftype], ftype)
        self.run_import_multiple_files(ftype, len(DATA_ARRAY[ftype]), additional_args)
        return self.run_export(ftype)

    @pytest.mark.parametrize("ftype,additional_args", ALL_PARAMS)
    def test_multiple_files_and_stdin(self, tmp_path, request, table_type, ftype, additional_args):
        self.init_test(tmp_path, table_type, request.node.name)
        self.write_array_to_files(DATA_ARRAY[ftype], ftype)
        self.run_import_multiple_files(ftype, len(DATA_ARRAY[ftype]), additional_args)
        return self.run_export(ftype)

    @pytest.mark.parametrize("ftype,additional_args", ONLY_CSV_TSV_PARAMS)
    def test_multiple_files_and_columns_opt(self, tmp_path, request, table_type, ftype, additional_args):
        self.init_test(tmp_path, table_type, request.node.name)
        self.write_array_to_files(DATA_ARRAY_BAD_HEADER[ftype], ftype)
        self.run_import_multiple_files(ftype, len(DATA_ARRAY_BAD_HEADER[ftype]), ["--columns", self.get_header(ftype)] + additional_args)
        return self.run_export(ftype)

    @pytest.mark.parametrize("ftype,additional_args", ALL_PARAMS)
    def test_big_dataset(self, tmp_path, request, table_type, ftype, additional_args):
        self.init_test(tmp_path, table_type, request.node.name)
        self.gen_dataset(DATASET_SIZE, FILES_COUNT, ftype)
        self.run_import_multiple_files(ftype, FILES_COUNT, additional_args)
        return self.validate_gen_data()

    @pytest.mark.skip("test is failing right now")
    def test_format_parquet(self, tmp_path, request, table_type):
        self.init_test(tmp_path, table_type, request.node.name)
        self.run_import_parquet(DATA_PARQUET)
        return self.run_export("csv")

    @pytest.mark.parametrize("ftype,additional_args", ALL_PARAMS)
    def test_import_file_with_bom(self, tmp_path, request, table_type, ftype, additional_args):
        self.init_test(tmp_path, table_type, request.node.name)
        self.run_import(ftype, DATA[ftype], additional_args, True)
        return self.run_export(ftype)

    @pytest.mark.parametrize("ftype,additional_args", ALL_PARAMS)
    def test_import_stdin_with_bom(self, tmp_path, request, table_type, ftype, additional_args):
        self.init_test(tmp_path, table_type, request.node.name)
        self.run_import_from_stdin(ftype, DATA[ftype], additional_args, True)
        return self.run_export(ftype)

    @pytest.mark.parametrize("ftype", ["csv"])
    def test_import_minimal_parallelism(self, tmp_path, request, table_type, ftype):
        """
        Tests import of 5 rows with minimal values for threads, max-in-flight, and batch-bytes.
        """
        self.init_test(tmp_path, table_type, request.node.name)
        # Use only the header and first 5 rows from DATA[ftype]
        data_lines = DATA[ftype].splitlines(keepends=True)
        data = ''.join(data_lines[:6])
        self.run_import(
            ftype,
            data,
            additional_args=["--threads", "1", "--max-in-flight", "1", "--batch-bytes", "1"]
        )
        return self.run_export(ftype)

    def _prepare_infer_input(self, tmp_path, request):
        self.tmp_path = tmp_path
        self.table_path = self.root_dir + "/" + request.node.name
        csv_path = self.tmp_path / "infer_input.csv"
        self.write_data_to_tmp_file(csv_path, False, DATA["csv"])
        return csv_path

    def _verify_table_exists(self, table_path):
        query = "SELECT count(*) FROM `{}`".format(table_path)
        self.execute_ydb_cli_command(["table", "query", "execute", "-q", query, "-t", "scan"])

    def test_tools_infer_csv_execute(self, tmp_path, request, table_type):
        csv_path = self._prepare_infer_input(tmp_path, request)
        result = self.execute_ydb_cli_command([
            "tools", "infer", "csv",
            "-p", self.table_path,
            "--execute",
            "--header",
            str(csv_path),
        ])
        assert "CREATE TABLE" in result.stderr
        assert "PRIMARY KEY" in result.stderr
        assert "key" in result.stderr
        assert "id" in result.stderr
        assert "value" in result.stderr
        self._verify_table_exists(self.table_path)

    def test_tools_infer_csv_no_execute(self, tmp_path, request, table_type):
        csv_path = self._prepare_infer_input(tmp_path, request)
        result = self.execute_ydb_cli_command([
            "tools", "infer", "csv",
            "-p", self.table_path,
            "--header",
            str(csv_path),
        ])
        assert "CREATE TABLE" in result.stdout
        assert "PRIMARY KEY" in result.stdout
        assert "key" in result.stdout
        assert "id" in result.stdout
        assert "value" in result.stdout

    def test_tools_infer_csv_execute_with_profile(self, tmp_path, request, table_type):
        csv_path = self._prepare_infer_input(tmp_path, request)

        profile_file = str(self.tmp_path / "profile.yaml")
        profile_content = {
            "profiles": {
                "test_profile": {
                    "endpoint": self.grpc_endpoint(),
                    "database": self.root_dir,
                },
            },
            "active_profile": "test_profile",
        }
        with open(profile_file, "w") as f:
            yaml.dump(profile_content, f)

        result = yatest.common.execute([
            ydb_bin(),
            "--profile-file", profile_file,
            "tools", "infer", "csv",
            "-p", self.table_path,
            "--execute",
            "--header",
            str(csv_path),
        ])

        result = result.std_err.decode("utf-8")
        assert "CREATE TABLE" in result
        assert "PRIMARY KEY" in result
        assert "key" in result
        assert "id" in result
        assert "value" in result
        self._verify_table_exists(self.table_path)
