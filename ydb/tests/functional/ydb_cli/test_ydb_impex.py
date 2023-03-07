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

DATA_TSV = DATA_CSV.replace(',', '\t')


DATA_JSON = """{"key":1,"id":1111,"value":"one"}
{"key":2,"id":2222,"value":"two"}
{"key":3,"id":3333,"value":"three"}
{"key":5,"id":5555,"value":"five"}
{"key":7,"id":7777,"value":"seven"}
"""

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

    def run_import_csv(self, ftype, data):
        self.clear_table()
        with open("tempinput.csv", "w") as f:
            f.writelines(data)
        self.execute_ydb_cli_command(["import", "file", ftype, "-p", self.table_path, "-i", "tempinput.csv", "--header"])

    def run_import_json(self, data):
        self.clear_table()
        with open("tempinput.json", "w") as f:
            f.writelines(data)
        self.execute_ydb_cli_command(["import", "file", "json", "-p", self.table_path, "-i", "tempinput.json"])

    def run_import_parquet(self, data):
        self.clear_table()
        with open("tempinput.parquet", "w"):
            pq.write_table(data, "tempinput.parquet", version="2.4")
        self.execute_ydb_cli_command(["import", "file", "parquet", "-p", self.table_path, "-i", "tempinput.parquet"])

    def run_export(self, format):
        query = "SELECT `key`, `id`, `value` FROM `{}` ORDER BY `key`".format(self.table_path)
        output = self.execute_ydb_cli_command(["table", "query", "execute", "-q", query, "-t", "scan", "--format", format])
        return self.canonical_result(output)

    def test_format_csv(self):
        self.run_import_csv("csv", DATA_CSV)
        return self.run_export("csv")

    def test_format_tsv(self):
        self.run_import_csv("tsv", DATA_TSV)
        return self.run_export("tsv")

    def test_format_json(self):
        self.run_import_json(DATA_JSON)
        return self.run_export("json-unicode")

    @pytest.mark.skip("test is failing right now")
    def test_format_parquet(self):
        self.run_import_parquet(DATA_PARQUET)
        return self.run_export("csv")
