# -*- coding: utf-8 -*-

from ydb.tests.library.common import yatest_common
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.oss.canonical import set_canondata_root
from ydb.tests.oss.ydb_sdk_import import ydb

import os
import logging
import pathlib

logger = logging.getLogger(__name__)


def ydb_bin():
    if os.getenv("YDB_CLI_BINARY"):
        return yatest_common.binary_path(os.getenv("YDB_CLI_BINARY"))
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


class BaseTestFlameGraphService(object):
    @classmethod
    def execute_ydb_cli_command(cls, args, stdin=None):
        try:
            execution = yatest_common.execute([ydb_bin()] + args, stdin=stdin)
            result = execution.std_out.decode('utf-8') + '\n' + execution.std_err.decode('utf-8')
        except Exception as exc:
            result = str(exc)
        logger.debug("result:\n" + result)
        return result

    @staticmethod
    def canonical_result(output_result):
        output_file_name = "result.output"
        with open(output_file_name, "w") as f:
            f.write(output_result.decode('utf-8'))
        return yatest_common.canonical_file(output_file_name, local=True, universal_lines=True)


class BaseTestFlameGraphServiceWithDatabase(BaseTestFlameGraphService):
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


class TestExecuteWithFlameGraph(BaseTestFlameGraphServiceWithDatabase):
    @classmethod
    def setup_class(cls):
        BaseTestFlameGraphServiceWithDatabase.setup_class()

        session = cls.driver.table_client.session().create()
        cls.table_path = cls.root_dir + "/flame_graph_options"
        create_table_with_data(session, cls.table_path)

    def check_output(self, output: str, expected_str: str):
        assert output.find(expected_str) != -1, f"Expected string {expected_str} not found"

    def check_svg(self, svg_file):
        svg_found = False
        with open(svg_file) as f:
            if '<title>Query TASKS' in f.read():
                svg_found = True

        pathlib.Path(svg_file).unlink()
        assert svg_found, 'Expected pattern is not found in ' + svg_file

    def yql_command(self, stats_mode: str, flame_graph_path: str, expected_str: str):
        script = "SELECT * FROM `{path}` WHERE key < 4;".format(path=self.table_path)
        output = self.execute_ydb_cli_command_with_db(["yql", "-s", script,
                                                       "--stats", stats_mode, "--flame-graph", flame_graph_path])
        self.check_output(output, expected_str)

    def scripting_yql_command(self, stats_mode: str, flame_graph_path: str, expected_str: str):
        script = "SELECT * FROM `{path}` WHERE key < 4;".format(path=self.table_path)
        output = self.execute_ydb_cli_command_with_db(["scripting", "yql", "-s", script,
                                                       "--stats", stats_mode, "--flame-graph", flame_graph_path])
        self.check_output(output, expected_str)

    def table_execute_command(self, stats_mode: str, flame_graph_path: str, expected_str: str):
        script = "SELECT * FROM `{path}` WHERE key < 4;".format(path=self.table_path)
        output = self.execute_ydb_cli_command_with_db(["scripting yql", "-s", script,
                                                       "--stats", stats_mode, "--flame-graph", flame_graph_path])
        self.check_output(output, expected_str)

    def table_explain(self, stats_mode: str, flame_graph_path: str, expected_str: str):
        script = "SELECT * FROM `{path}` WHERE key < 4;".format(path=self.table_path)
        output = self.execute_ydb_cli_command_with_db(["scripting yql", "-s", script,
                                                       "--stats", stats_mode, "--flame-graph", flame_graph_path])
        self.check_output(output, expected_str)

    def test_fg_with_full_stats(self):
        self.yql_command('full', 'test_fg.svg',
                         'Resource usage flame graph is successfully saved to test_fg.svg')
        self.check_svg('test_fg.svg')

        self.scripting_yql_command('full', 'test_fg.svg',
                                   'Resource usage flame graph is successfully saved to test_fg.svg')
        self.check_svg('test_fg.svg')

    def test_fg_with_profile_stats(self):
        self.yql_command('profile', 'test_fg.svg',
                         'Resource usage flame graph is successfully saved to test_fg.svg')
        self.check_svg('test_fg.svg')

        self.scripting_yql_command('profile', 'test_fg.svg',
                                   'Resource usage flame graph is successfully saved to test_fg.svg')
        self.check_svg('test_fg.svg')

    def test_fg_with_basic_stats(self):
        self.yql_command('basic', 'test_fg.svg',
                         'Flame graph is available for full or profile stats. Current: basic.')

        self.scripting_yql_command('basic', 'test_fg.svg',
                                   'Flame graph is available for full or profile stats. Current: basic.')

    def test_fg_to_dir(self):
        pathlib.Path('aaa').mkdir()
        self.yql_command('full', 'aaa',
                         'Can\'t save resource usage flame graph, error: aaa is a directory')

        self.scripting_yql_command('full', 'aaa',
                                   'Can\'t save resource usage flame graph, error: aaa is a directory')
