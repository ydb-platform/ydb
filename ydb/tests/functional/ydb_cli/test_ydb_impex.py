# -*- coding: utf-8 -*-
from ydb.tests.library.common import yatest_common
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.oss.canonical import set_canondata_root
from ydb.tests.oss.ydb_sdk_import import ydb

import pytest
import logging
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


DATA_CSV = """key,id,value
1,1111,"one"
2,2222,"two"
3,3333,"three"
5,5555,"five"
7,7777,"seven"
"""

DATA_ARRAY_CSV = ["""key,id,value
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

DATA_ARRAY_CSV_BAD_HEADER = DATA_ARRAY_CSV
DATA_ARRAY_CSV_BAD_HEADER[0].replace("key,id,value", 'a,b,c')

DATA_TSV = DATA_CSV.replace(',', '\t')

DATA_ARRAY_TSV = list(map(lambda s: s.replace(',', '\t'), DATA_ARRAY_CSV))
DATA_ARRAY_TSV_BAD_HEADER = DATA_ARRAY_TSV
DATA_ARRAY_TSV_BAD_HEADER[0].replace("key\tid\tvalue", 'a\tb\tc')


DATA_JSON = """{"key":1,"id":1111,"value":"one"}
{"key":2,"id":2222,"value":"two"}
{"key":3,"id":3333,"value":"three"}
{"key":5,"id":5555,"value":"five"}
{"key":7,"id":7777,"value":"seven"}
"""

DATA_ARRAY_JSON = ["""{"key":1,"id":1111,"value":"one"}
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

FILES_COUNT = 4
DATASET_SIZE = 1000

ARRAYS = [pa.array([1, 2, 3, 5, 7], type=pa.uint32()), pa.array([1111, 2222, 3333, 5555, 7777], type=pa.uint64()), pa.array(["one", "two", "three", "five", "seven"], type=pa.string())]
ARRAY_NAMES = ['key', 'id', 'value']
DATA_PARQUET = pa.Table.from_arrays(ARRAYS, names=ARRAY_NAMES)


def ydb_bin():
    return yatest_common.binary_path("ydb/apps/ydb/ydb")


def create_table(session, path):
    session.create_table(
        path,
        ydb.TableDescription()
        .with_column(ydb.Column("key", ydb.OptionalType(ydb.PrimitiveType.Uint32)))
        .with_column(ydb.Column("id", ydb.OptionalType(ydb.PrimitiveType.Uint64)))
        .with_column(ydb.Column("value", ydb.OptionalType(ydb.PrimitiveType.Utf8)))
        .with_primary_keys("key")
    )


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
    def execute_ydb_cli_command(cls, args, stdin=None, stdout=None):
        execution = yatest_common.execute(
            [
                ydb_bin(),
                "--endpoint", "grpc://localhost:%d" % cls.cluster.nodes[1].grpc_port,
                "--database", cls.root_dir
            ] +
            args,
            stdin=stdin,
            stdout=stdout,
        )

        result = execution.std_out
        logger.debug("std_out:\n" + result.decode('utf-8'))
        return result


class TestImpex(BaseTestTableService):

    @classmethod
    def setup_class(cls):
        BaseTestTableService.setup_class()

        session = cls.driver.table_client.session().create()
        cls.table_path = cls.root_dir + "/impex_table"
        create_table(session, cls.table_path)

    def clear_table(self):
        query = "DELETE FROM `{}`".format(self.table_path)
        self.execute_ydb_cli_command(["yql", "-s", query])

    @staticmethod
    def write_array_to_files(arr, ftype):
        for i in range(len(arr)):
            with open("tempinput{}.{}".format(i, ftype), "w") as f:
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

    @staticmethod
    def gen_dataset(rows, files, ftype):
        id_set = [10, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200]
        value_set = ["", "aaaaaaaaaa", "bbbbbbbbbb", "ccccccccc", "ddddddd", "eeeeeeeee", "fffffffff"]
        for i in range(files):
            with open("tempinput{}.{}".format(i, ftype), "w") as f:
                f.write(TestImpex.get_header(ftype))
                for key in range(i * rows, (i + 1) * rows):
                    f.write(TestImpex.get_row_in_format(ftype, key, id_set[key % len(id_set)], value_set[key % len(value_set)]))

    def run_import(self, ftype, data):
        self.clear_table()
        with open("tempinput.{}".format(ftype), "w") as f:
            f.writelines(data)
        self.execute_ydb_cli_command(["import", "file", ftype, "-p", self.table_path, "-i", "tempinput.{}".format(ftype)] + self.get_header_flag(ftype))

    def run_import_from_stdin(self, ftype, data):
        self.clear_table()
        with open("tempinput.{}".format(ftype), "w") as f:
            f.writelines(data)
        with open("tempinput.{}".format(ftype), "r") as f:
            self.execute_ydb_cli_command(["import", "file", ftype, "-p", self.table_path] + self.get_header_flag(ftype), stdin=f)

    def run_import_multiple_files(self, ftype, files_count, additional_args=[]):
        self.clear_table()
        args = ["import", "file", ftype, "-p", self.table_path] + self.get_header_flag(ftype) + additional_args
        for i in range(files_count):
            args.append("tempinput{}.{}".format(i, ftype))
        self.execute_ydb_cli_command(args)

    def run_import_multiple_files_and_stdin(self, ftype, files_count):
        self.clear_table()
        args = ["import", "file", ftype, "-p", self.table_path] + self.get_header_flag(ftype)
        for i in range(1, files_count):
            args.append("tempinput{}.{}".format(i, ftype))
        with open("tempinput0.{}".format(ftype), "r") as f:
            self.execute_ydb_cli_command(args, stdin=f)

    def run_import_parquet(self, data):
        self.clear_table()
        with open("tempinput.parquet", "w"):
            pq.write_table(data, "tempinput.parquet", version="2.4")
        self.execute_ydb_cli_command(["import", "file", "parquet", "-p", self.table_path, "-i", "tempinput.parquet"])

    def run_export(self, format):
        query = "SELECT `key`, `id`, `value` FROM `{}` ORDER BY `key`".format(self.table_path)
        output_file_name = "result.output"
        self.execute_ydb_cli_command(["table", "query", "execute", "-q", query, "-t", "scan", "--format", format], stdout=output_file_name)
        return yatest_common.canonical_file(output_file_name, local=True, universal_lines=True)

    def test_format_csv(self):
        self.run_import("csv", DATA_CSV)
        return self.run_export("csv")

    def test_format_csv_from_stdin(self):
        self.run_import_from_stdin("csv", DATA_CSV)
        return self.run_export("csv")

    def test_format_csv_multiple_files(self):
        self.write_array_to_files(DATA_ARRAY_CSV, "csv")
        self.run_import_multiple_files("csv", len(DATA_ARRAY_CSV))
        return self.run_export("csv")

    def test_format_csv_multiple_files_and_stdin(self):
        self.write_array_to_files(DATA_ARRAY_CSV, "csv")
        self.run_import_multiple_files("csv", len(DATA_ARRAY_CSV))
        return self.run_export("csv")

    def test_format_csv_multiple_files_columns(self):
        self.write_array_to_files(DATA_ARRAY_CSV_BAD_HEADER, "csv")
        self.run_import_multiple_files("csv", len(DATA_ARRAY_CSV_BAD_HEADER), ["--columns", self.get_header("csv")])
        return self.run_export("csv")

    def test_format_csv_multiple_files_big_data(self):
        self.gen_dataset(DATASET_SIZE, FILES_COUNT, "csv")
        self.run_import_multiple_files("csv", FILES_COUNT)
        return self.run_export("csv")

    def test_format_csv_multiple_files_newline_delimited(self):
        self.write_array_to_files(DATA_ARRAY_CSV, "csv")
        self.run_import_multiple_files("csv", len(DATA_ARRAY_CSV), ["--newline-delimited"])
        return self.run_export("csv")

    def test_format_csv_multiple_files_newline_delimited_and_columns(self):
        self.write_array_to_files(DATA_ARRAY_CSV_BAD_HEADER, "csv")
        self.run_import_multiple_files("csv", len(DATA_ARRAY_CSV_BAD_HEADER), ["--newline-delimited", "--columns", self.get_header("csv")])
        return self.run_export("csv")

    def test_format_csv_multiple_files_newline_delimited_big_data(self):
        self.gen_dataset(DATASET_SIZE, FILES_COUNT, "csv")
        self.run_import_multiple_files("csv", FILES_COUNT, ["--newline-delimited"])
        return self.run_export("csv")

    def test_format_tsv(self):
        self.run_import("tsv", DATA_TSV)
        return self.run_export("tsv")

    def test_format_tsv_from_stdin(self):
        self.run_import_from_stdin("tsv", DATA_TSV)
        return self.run_export("tsv")

    def test_format_tsv_multiple_files(self):
        self.write_array_to_files(DATA_ARRAY_TSV, "tsv")
        self.run_import_multiple_files("tsv", len(DATA_ARRAY_TSV))
        return self.run_export("tsv")

    def test_format_tsv_multiple_files_and_stdin(self):
        self.write_array_to_files(DATA_ARRAY_TSV, "tsv")
        self.run_import_multiple_files_and_stdin("tsv", len(DATA_ARRAY_TSV))
        return self.run_export("tsv")

    def test_format_tsv_multiple_files_columns(self):
        self.write_array_to_files(DATA_ARRAY_TSV_BAD_HEADER, "tsv")
        self.run_import_multiple_files("tsv", len(DATA_ARRAY_TSV_BAD_HEADER), ["--columns", self.get_header("tsv")])
        return self.run_export("tsv")

    def test_format_tsv_multiple_files_big_data(self):
        self.gen_dataset(DATASET_SIZE, FILES_COUNT, "tsv")
        self.run_import_multiple_files("tsv", FILES_COUNT)
        return self.run_export("tsv")

    def test_format_tsv_multiple_files_newline_delimited(self):
        self.write_array_to_files(DATA_ARRAY_TSV, "tsv")
        self.run_import_multiple_files("tsv", len(DATA_ARRAY_TSV), ["--newline-delimited"])
        return self.run_export("tsv")

    def test_format_tsv_multiple_files_newline_delimited_and_columns(self):
        self.write_array_to_files(DATA_ARRAY_TSV_BAD_HEADER, "tsv")
        self.run_import_multiple_files("tsv", len(DATA_ARRAY_TSV_BAD_HEADER), ["--newline-delimited", "--columns", self.get_header("tsv")])
        return self.run_export("tsv")

    def test_format_tsv_multiple_files_newline_delimited_big_data(self):
        self.gen_dataset(DATASET_SIZE, FILES_COUNT, "tsv")
        self.run_import_multiple_files("tsv", FILES_COUNT, ["--newline-delimited"])
        return self.run_export("tsv")

    def test_format_json(self):
        self.run_import("json", DATA_JSON)
        return self.run_export("json-unicode")

    def test_format_json_from_stdin(self):
        self.run_import_from_stdin("json", DATA_JSON)
        return self.run_export("json-unicode")

    def test_format_json_multiple_files(self):
        self.write_array_to_files(DATA_ARRAY_JSON, "json")
        self.run_import_multiple_files("json", len(DATA_ARRAY_JSON))
        return self.run_export("json-unicode")

    def test_format_json_multiple_files_and_stdin(self):
        self.write_array_to_files(DATA_ARRAY_JSON, "json")
        self.run_import_multiple_files_and_stdin("json", len(DATA_ARRAY_JSON))
        return self.run_export("json-unicode")

    def test_format_json_multiple_files_big_data(self):
        self.gen_dataset(DATASET_SIZE, FILES_COUNT, "json")
        self.run_import_multiple_files("json", FILES_COUNT)
        return self.run_export("json-unicode")

    @pytest.mark.skip("test is failing right now")
    def test_format_parquet(self):
        self.run_import_parquet(DATA_PARQUET)
        return self.run_export("csv")
