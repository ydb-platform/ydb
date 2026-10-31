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
            "window.toggleChartTile = toggleChartTile;",
        )
        missing = [name for name in required if name not in source]
        self.assertEqual(missing, [], msg=f"missing window exports: {missing}")

    def test_dashboard_html_uses_chart_tiles_and_collapses_tests_by_suite(self):
        source = (_DIR / "dashboard_html_main.py").read_text(encoding="utf-8")
        self.assertIn('class="chart-grid"', source)
        self.assertIn('class="chart-tile"', source)
        self.assertIn("function toggleChartTile(btn)", source)
        self.assertIn("function syncChartGroupXRange(ids)", source)
        self.assertIn("function fullEvlogRangeMs()", source)
        self.assertIn('id="testsBySuiteDetails"', source)
        self.assertIn('id="activeBySuiteDetails"', source)
        self.assertIn("Tests in parallel by suite", source)
        self.assertIn("Active chunks by suite", source)
        # Runner monitoring lives inside the dashboard header, not as a
        # standalone box between Config and CPU suggestions.
        header_idx = source.find('id="dashboardHeader"')
        monitoring_idx = source.find('id="monitoringLinkWrap"')
        headline_idx = source.find('id="headlineStats"')
        self.assertGreater(header_idx, 0)
        self.assertGreater(monitoring_idx, header_idx)
        self.assertGreater(headline_idx, monitoring_idx)
        header_block = source[header_idx:headline_idx]
        self.assertIn('id="monitoringLinkWrap"', header_block)
        self.assertIn("Runner monitoring:", header_block)
        self.assertIn("build duration", source)
        self.assertIn("test duration", source)
        self.assertIn("function resolvePhaseDurations(hs)", source)
        self.assertIn("CPU by suite · estimated cores", source)
        self.assertIn("chart-tile-sub", source)
        # Expand matches the ya-activity full-row slot (same as .span-2).
        self.assertIn(".chart-tile.span-2,\n    .chart-tile.expanded {{", source)
        self.assertIn("grid-column: 1 / -1;", source)
        self.assertIn("height: 520px;", source)
        self.assertIn("Expand to full width (same as ya make concurrency)", source)
        # Hover/click panels must remain visible in compact tile mode.
        self.assertIn(
            "Hover/click detail panels stay visible in tile mode too",
            source,
        )
        self.assertNotIn(
            ".chart-tile-meta {{\n      display: none;",
            source,
        )
        # Suite grid → tests кат → active кат → pies. Both secondary charts
        # sit under closed <details>, not in the main suite grid.
        suite_grid_idx = source.find('id="suiteChartGrid"')
        tests_details_idx = source.find('id="testsBySuiteDetails"')
        active_details_idx = source.find('id="activeBySuiteDetails"')
        tests_pane_idx = source.find('id="testsLayerSuite"')
        active_pane_idx = source.find('id="activeLayerSuite"')
        pies_idx = source.find('id="cpuPieSuite"')
        self.assertGreater(suite_grid_idx, 0)
        self.assertGreater(tests_details_idx, suite_grid_idx)
        self.assertGreater(active_details_idx, tests_details_idx)
        self.assertGreater(pies_idx, active_details_idx)
        self.assertGreater(tests_pane_idx, tests_details_idx)
        self.assertLess(tests_pane_idx, active_details_idx)
        self.assertGreater(active_pane_idx, active_details_idx)
        self.assertLess(active_pane_idx, pies_idx)
        # Active must not remain a tile inside the main suite grid.
        suite_grid_block = source[suite_grid_idx:tests_details_idx]
        self.assertNotIn('data-pane="activeLayerSuite"', suite_grid_block)
        self.assertNotIn('id="activeLayerSuite"', suite_grid_block)


if __name__ == "__main__":
    unittest.main()
