# -*- coding: utf-8 -*-

from ydb.tests.library.common import yatest_common
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.oss_canonical import set_canondata_root

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


class BaseTestScriptingService(object):
    @classmethod
    def execute_ydb_cli_command(cls, args):
        execution = yatest_common.execute([ydb_bin()] + args)
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
    def execute_ydb_cli_command_with_db(cls, args):
        return cls.execute_ydb_cli_command(
            [
                "--endpoint", "grpc://localhost:%d" % cls.cluster.nodes[1].grpc_port,
                "--database", cls.root_dir
            ] +
            args
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
