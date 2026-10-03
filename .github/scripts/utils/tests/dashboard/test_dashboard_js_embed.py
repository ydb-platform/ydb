"""JSON embed helpers must stay outside f-strings (Python < 3.12)."""

from __future__ import annotations

import ast
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _paths import TEST_METRICS, add_product_paths

add_product_paths(TEST_METRICS)

from html_embed import js_script_json

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


if __name__ == "__main__":
    unittest.main()
