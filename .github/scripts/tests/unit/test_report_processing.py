"""Regression checks for CI report validation and published log links."""

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))


def load_script(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


checker = load_script("fail-checker")
# These checks do not query test history or require a database connection.
with patch.dict(sys.modules, {"get_test_history": Mock()}):
    summary = load_script("generate-summary")
transformer = load_script("transform_build_results")


class ReportProcessingTests(unittest.TestCase):
    def test_invalid_reports_are_infrastructure_errors(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            count = Path(tmp) / "count.txt"
            for content in (None, "{", "{}", '[]', '{"results": null}', '{"results": [null]}',
                            '{"results": [{}]}', '{"results": [{"status": "NEW_STATUS"}]}'):
                with self.subTest(content=content):
                    if content is not None:
                        report.write_text(content)
                    with self.assertRaises(SystemExit) as error:
                        checker.check_for_fail([str(report)], str(count))
                    self.assertEqual(error.exception.code, 2)
                    self.assertFalse(count.exists())

    def test_empty_selection_and_known_statuses(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            count = Path(tmp) / "count.txt"
            for statuses in ([], ["PASSED", "MUTE", "SKIPPED"], ["FAILED", "ERROR"]):
                with self.subTest(statuses=statuses):
                    report.write_text(json.dumps({"results": [{"status": s} for s in statuses]}))
                    if "FAILED" in statuses:
                        with self.assertRaises(SystemExit) as error:
                            checker.check_for_fail([str(report)], str(count))
                        self.assertEqual(error.exception.code, -1)
                        self.assertEqual(count.read_text(), "2")
                    else:
                        checker.check_for_fail([str(report)], str(count))
                        self.assertEqual(count.read_text(), "0")

    def test_summary_does_not_treat_unknown_status_as_pass(self):
        for status in ("NEW_STATUS", "", "NOT_LAUNCHED"):
            with self.subTest(status=status), self.assertRaises(ValueError):
                summary.TestResult.from_build_results_report({"path": "suite", "status": status})
        result = summary.TestResult.from_build_results_report({"path": "suite", "status": "PASSED"})
        self.assertEqual(result.status, summary.TestStatus.PASS)

    def test_transform_does_not_turn_invalid_report_into_empty_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            for content in ("{}", "[]", '{"results": null}', '{"results": [null]}'):
                report.write_text(content)
                with self.subTest(content=content), self.assertRaises(ValueError):
                    transformer.transform(str(report), lambda *_: False, tmp, "", tmp, 0, tmp, "", None)
                self.assertEqual(report.read_text(), content)

    def test_only_existing_nonempty_logs_have_links(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out = root / "out"
            logs = root / "logs"
            out.mkdir()
            logs.mkdir()
            (out / "stderr").write_text("failure details")
            (out / "stdout").touch()
            report = root / "report.json"
            report.write_text(json.dumps({"results": [{
                "path": "suite", "name": "case", "type": "test", "status": "FAILED",
                "links": {"stderr": [str(out / "stderr")], "stdout": [str(out / "stdout")],
                          "log": [str(out / "missing")]},
            }]}))
            with patch.object(transformer, "enrich_error_types_in_results"):
                transformer.transform(str(report), lambda *_: False, str(out), "https://logs/",
                                      str(logs), 0, str(root / "zips"), "https://zips/", None)
            links = json.loads(report.read_text())["results"][0]["links"]
            self.assertEqual(links, {"stderr": ["https://logs/stderr"]})
            self.assertEqual((logs / "stderr").read_text(), "failure details")


if __name__ == "__main__":
    unittest.main()
