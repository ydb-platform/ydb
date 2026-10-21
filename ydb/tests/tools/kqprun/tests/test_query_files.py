import pathlib
import subprocess
import tempfile
import unittest

import yatest.common


class TestQueryFiles(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="kqprun external queries ", dir=yatest.common.output_path())
        self.addCleanup(directory.cleanup)
        self.root = pathlib.Path(directory.name)

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return str(path)

    def run_queries(self, *args):
        return subprocess.run(
            [
                yatest.common.binary_path("ydb/tests/tools/kqprun/kqprun"),
                "--app-config",
                yatest.common.source_path("ydb/tests/tools/kqprun/tests/cfg/config.conf"),
                "--log-file",
                str(self.root / "ydb.log"),
                "--timeout",
                "10000",
                "--script-ast-file",
                str(self.root / "queries.ast"),
                *map(str, args),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=120,
        )

    def test_explain_external_directories(self):
        create = self.write("create.sql", "CREATE TABLE sample (key Uint64, PRIMARY KEY (key));")
        # Create files in the opposite order to their required execution order.
        drop = self.write("schema/20-drop.sql", "ALTER TABLE sample DROP COLUMN temporary;")
        add = self.write("schema/10-add.yql", "ALTER TABLE sample ADD COLUMN temporary String;")
        add_column = self.write("add-column.yql", "ALTER TABLE sample ADD COLUMN value String;")
        read = self.write("queries/20-read.sql", "DECLARE $key AS Uint64; SELECT value FROM sample WHERE key = $key;")
        write = self.write("queries/10-write.yql", "DECLARE $key AS Uint64; UPSERT INTO sample (key) VALUES ($key);")
        self.write("queries/ignored.txt", "invalid SQL")
        self.write("queries/nested/ignored.sql", "invalid SQL")
        (self.root / "queries/directory.sql").mkdir()
        extra = self.write("extra/30-read.yql", "SELECT key FROM sample;")
        last = self.write("last.sql", "SELECT value FROM sample;")

        result = self.run_queries(
            "--sql",
            "SELECT 42;",
            "--script-query-dir",
            self.root / "queries",
            "--script-query-dir",
            self.root / "extra",
            "--script-query",
            last,
            "--scheme-query",
            create,
            "--scheme-query-dir",
            self.root / "schema",
            "--scheme-query",
            add_column,
            "--script-action",
            "explain",
        )

        self.assertEqual(result.returncode, 0, result.stdout)
        ordered_paths = [create, add, drop, add_column, write, read, extra, last]
        positions = [result.stdout.index(path) for path in ordered_paths]
        self.assertEqual(positions, sorted(positions), result.stdout)
        self.assertEqual(result.stdout.count("Executing query"), 5, result.stdout)
        self.assertNotIn("ignored", result.stdout)

    def test_continue_after_failed_explain(self):
        failed = self.write("queries/10-broken.sql", "SELECT * FROM missing_table;")
        succeeded = self.write("queries/20-valid.yql", "DECLARE $value AS Uint64; SELECT $value;")
        result = self.run_queries(
            "--script-query-dir",
            self.root / "queries",
            "-A",
            "explain",
            "--continue-after-fail",
        )

        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(f"Query 0 ('{failed}') failed:", result.stdout)
        self.assertIn(f"Executing query 1 ('{succeeded}')", result.stdout)
        self.assertIn("1 of 2 query requests failed", result.stdout)

    def test_results_printed_after_failure(self):
        result = self.run_queries(
            "--sql",
            "SELECT * FROM missing_table;",
            "--sql",
            "SELECT 1234567 AS answer;",
            "--result-file",
            "-",
            "--continue-after-fail",
        )

        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("Query 0 failed:", result.stdout)
        self.assertIn("1234567", result.stdout)
        self.assertIn("1 of 2 query requests failed", result.stdout)

    def test_schema_failure_stops_before_queries(self):
        failed = self.write("schema/10-broken.sql", "invalid SQL")
        query = self.write("queries/10-valid.sql", "SELECT 42;")
        result = self.run_queries(
            "--scheme-query-dir",
            self.root / "schema",
            "--script-query-dir",
            self.root / "queries",
            "-A",
            "explain",
            "--continue-after-fail",
        )

        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(f"Scheme query '{failed}' execution failed", result.stdout)
        self.assertNotIn(query, result.stdout)

    def test_scheme_limits(self):
        schema = self.write(
            "schema.sql", "CREATE TABLE sample (key Uint64, value String, extra String, PRIMARY KEY (key));"
        )
        limits = self.write("limits.pb.txt", "MaxTableColumns: 2")
        result = self.run_queries("--scheme-limits", limits, "--scheme-query", schema, "--sql", "SELECT 42;")
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("Too many columns", result.stdout)

        pathlib.Path(limits).write_text("MaxTableColumns: 3")
        result = self.run_queries("--scheme-limits", limits, "--scheme-query", schema, "--sql", "SELECT 42;")
        self.assertEqual(result.returncode, 0, result.stdout)

        pathlib.Path(limits).write_text("MaxTableColumns: 2")
        result = self.run_queries(
            "--scheme-limits",
            limits,
            "--dedicated",
            "tenant",
            "-D",
            "/Root/tenant",
            "-C",
            "query",
            "--sql",
            pathlib.Path(schema).read_text(),
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("Too many columns", result.stdout)

        pathlib.Path(limits).write_text("invalid limits")
        result = self.run_queries("--scheme-limits", limits, "--sql", "SELECT 42;")
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(f"Bad format of scheme limits file '{limits}'", result.stdout)
        self.assertNotIn("Initialization of kqp runner", result.stdout)

    def test_invalid_directories(self):
        self.write("empty/ignored.txt", "SELECT 42;")
        self.write("empty/nested/query.sql", "SELECT 42;")
        file_path = self.write("file.sql", "SELECT 42;")
        for option in ("--scheme-query-dir", "--script-query-dir"):
            for path in (self.root / "empty", self.root / "missing", file_path):
                with self.subTest(option=option, path=path):
                    result = self.run_queries(option, path)
                    self.assertNotEqual(result.returncode, 0, result.stdout)
                    self.assertIn(str(path), result.stdout)
                    self.assertNotIn("Initialization of kqp runner", result.stdout)
