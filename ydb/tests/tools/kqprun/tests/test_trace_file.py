import json
import pathlib
import subprocess
import tempfile
import unittest

import yatest.common


class TestTraceFile(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="kqprun traces ", dir=yatest.common.output_path())
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

    def test_trace_file(self):
        trace = self.root / "spans.jsonl"
        result = self.run_queries(
            "--sql",
            "SELECT 42;",
            "-C",
            "query",
            "-A",
            "explain",
            "--trace-file",
            trace,
            "--trace-dataset",
            "example run",
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        documents = [json.loads(line) for line in trace.read_text().splitlines()]
        spans = []
        for doc in documents:
            resource = doc["resourceSpans"][0]
            self.assertEqual(resource["resource"]["attributes"][0]["value"]["stringValue"], "example run")
            spans.extend(resource["scopeSpans"][0]["spans"])
        self.assertEqual(sum(s["name"] == "Compile query" for s in spans), 1)
        self.assertTrue(all(int(s["endTimeUnixNano"]) >= int(s["startTimeUnixNano"]) for s in spans))

        result = self.run_queries("--sql", "SELECT 42;", "--trace-level", "16")
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("Trace level must be between 1 and 15", result.stdout)

    def test_trace_label_survives_redaction(self):
        query = self.write("secret-marker.sql", "SELECT 'password' AS marker;")
        trace = self.root / "redacted.jsonl"
        result = self.run_queries("-p", query, "-C", "query", "-A", "explain", "--trace-file", trace)
        self.assertEqual(result.returncode, 0, result.stdout)
        spans = [
            s
            for line in trace.read_text().splitlines()
            for rs in json.loads(line)["resourceSpans"]
            for scope in rs["scopeSpans"]
            for s in scope["spans"]
        ]
        request = next(s for s in spans if s["name"] == "kqprun.query")
        attrs = {a["key"]: a["value"] for a in request["attributes"]}
        self.assertEqual(attrs["query.name"]["stringValue"], "secret-marker.sql")
        self.assertTrue(any(s["name"] == "Compile query" for s in spans))
        texts = [
            a["value"]["stringValue"] for s in spans for a in s.get("attributes", []) if a["key"] == "db.query.text"
        ]
        self.assertTrue(texts)
        self.assertTrue(all("hidden" in t for t in texts), texts)
