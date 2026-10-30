"""JSON embed helpers must stay outside f-strings (Python < 3.12)."""

from __future__ import annotations

import ast
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _paths import TEST_METRICS, add_product_paths

add_product_paths(TEST_METRICS)

from html_embed import (
    js_script_json,
    sanitize_for_json,
    strip_suggestion_hover_text,
    write_dashboard_data_json,
)

_DIR = TEST_METRICS


class JsScriptJsonTest(unittest.TestCase):
    def test_escapes_script_close(self):
        raw = js_script_json({"x": "</script><script>alert(1)</script>"})
        self.assertNotIn("</script>", raw)
        self.assertNotIn("</SCRIPT>", raw)
        self.assertIn("\\u003c/script\\u003e", raw)
        upper = js_script_json({"x": "</SCRIPT>"})
        self.assertNotIn("</SCRIPT>", upper)
        self.assertIn("\\u003c/SCRIPT\\u003e", upper)

    def test_strip_suggestion_hover_text(self):
        rows = strip_suggestion_hover_text(
            [
                {
                    "suite_path": "a/b",
                    "recommended_cpu": 4,
                    "recommended_cpu_tooltip": "long text",
                    "mem_explain": "why",
                    "cpu_action": "raise",
                }
            ]
        )
        self.assertEqual(
            rows,
            [{"suite_path": "a/b", "recommended_cpu": 4, "cpu_action": "raise"}],
        )

    def test_write_dashboard_data_json_compact(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "dashboard_data.json"
            write_dashboard_data_json(path, {"a": 1, "b": [2, 3]})
            text = path.read_text(encoding="utf-8")
            self.assertEqual(text, '{"a":1,"b":[2,3]}')

    def test_sanitize_non_finite_floats_for_json_parse(self):
        import math

        cleaned = sanitize_for_json({"x": math.nan, "y": [math.inf, 1.5]})
        self.assertEqual(cleaned, {"x": None, "y": [None, 1.5]})
        raw = js_script_json({"x": math.nan})
        self.assertIn("null", raw)
        self.assertNotIn("NaN", raw)

    def test_cpu_seconds_keeps_seconds(self):
        from tests_resource_dashboard import cpu_seconds

        self.assertEqual(cpu_seconds({"ru_utime": 1500.0, "ru_stime": 500.0}), 2000.0)

    def test_html_templates_parse_without_backslash_in_fstring_expr(self):
        """CI runners still use Python 3.10/3.11; this is the smoke failure mode."""
        for name in ("dashboard_html_main.py", "dashboard_report_table.py"):
            source = (_DIR / name).read_text(encoding="utf-8")
            tree = ast.parse(source, filename=name)
            bad = []
            for node in ast.walk(tree):
                if not isinstance(node, ast.JoinedStr):
                    continue
                for part in node.values:
                    if not isinstance(part, ast.FormattedValue):
                        continue
                    snippet = ast.get_source_segment(source, part.value)
                    if snippet and "\\" in snippet:
                        bad.append((name, part.lineno, snippet))
            self.assertEqual(bad, [], msg=f"backslash in f-string expression: {bad}")

    def test_dashboard_html_exports_inline_handler_globals(self):
        source = (_DIR / "dashboard_html_main.py").read_text(encoding="utf-8")
        # Require the real post-fetch assignments, not the pre-fetch no-op stubs
        # (`window.showTab = function() {}`).
        required = (
            "window.toggleSuiteMarkers = toggleSuiteMarkers;",
            "window.showTab = showTab;",
            "window.clearSuiteSearch = clearSuiteSearch;",
            "window.clearAllMarkers = clearAllMarkers;",
            "window.applyTimezoneToAllCharts = applyTimezoneToAllCharts;",
            "window.applyMarkersToCharts = applyMarkersToCharts;",
            "window.applyLayerToggles = applyLayerToggles;",
        )
        missing = [name for name in required if name not in source]
        self.assertEqual(missing, [], msg=f"missing window exports: {missing}")


if __name__ == "__main__":
    unittest.main()
