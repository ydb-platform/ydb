# -*- coding: utf-8 -*-

from ydb.tests.functional.ydb_cli.ydb_cli_helpers import BaseCliTestWithDatabase
from ydb.tests.oss.ydb_sdk_import import ydb

import csv
import json
import os
import logging
import pathlib

logger = logging.getLogger(__name__)


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


class BaseTestFlameGraphServiceWithDatabase(BaseCliTestWithDatabase):
    @classmethod
    def execute_ydb_cli_command_with_db(cls, args, stdin=None):
        try:
            result = cls.execute_ydb_cli_command(args, stdin=stdin)
            return result.stdout + "\n" + result.stderr
        except Exception as exc:
            result = str(exc)
            logger.debug("result:\n" + result)
            return result


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

    def test_workload_query_reports(self, tmp_path):
        run_dir = tmp_path / "run"
        run_dir.mkdir()
        early_rows = f"FROM `{self.table_path}` WHERE `key` < 4"
        (run_dir / "rows.sql").write_text(f"SELECT `key`, `id`, `value` {early_rows} ORDER BY `key`;\n")
        (run_dir / "rows.sql.result").write_text("key,id,value\n1,1111,one\n2,2222,two\n3,3333,three\n")
        (run_dir / "totals.sql").write_text(f"SELECT COUNT(*) AS records, SUM(`id`) AS total {early_rows};\n")
        # The totals reference predates the third row and should fail the check.
        (run_dir / "totals.sql.result").write_text("records,total\n2,3333\n")
        (run_dir / "full_scan.sql").write_text(f"SELECT * FROM `{self.table_path}`;\n")

        csv_report = tmp_path / "summary.csv"
        json_report = tmp_path / "metrics.json"
        result = self.execute_ydb_cli_command([
            "workload", "query", "run", "--suite-path", str(tmp_path),
            "--include", "rows.sql,totals.sql", "--check-canonical", "--iterations", "2", "--threads", "1",
            "--output", str(tmp_path / "results"), "--csv", str(csv_report), "--json", str(json_report),
            "--ministat", str(tmp_path / "timings.csv"), "--plan", str(tmp_path / "plans" / "query"),
        ], check_exit_code=False)
        assert result.exit_code == 1, result.stdout + result.stderr

        expected = {"rows.sql": (2, 0, 0), "totals.sql": (2, 0, 1)}
        counters = ("SuccessCount", "FailsCount", "DiffsCount")
        with csv_report.open() as report:
            rows = {
                row["Query #"]: tuple(int(row[field] or 0) for field in counters)
                for row in csv.DictReader(report)
                if row["Query #"] not in ("Sum", "Avg", "GAvg")
            }
        assert rows == expected, result.stderr
        metrics = {
            (metric["labels"]["query"], metric["sensor"]): metric["value"]
            for metric in json.loads(json_report.read_text())
        }
        for query, counts in expected.items():
            assert tuple(metrics[query, field] for field in counters) == counts
            for execution in ("explain", "0", "1"):
                path = tmp_path / "plans" / f"query.{query}.{execution}.json"
                assert json.loads(path.read_text())["Plan"]

        timings = (tmp_path / "timings.csv").read_text().splitlines()
        assert len(timings) == len(expected)
        assert all(len(line.split(",")) == 2 for line in timings)
        assert "UNEXPECTED DIFF" in (tmp_path / "results.totals.sql.out").read_text()
