#!/usr/bin/env python3
"""Guard summary.html: every template variable must be a render() kwarg."""
from __future__ import annotations

import ast
import unittest
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, meta

_TESTS_DIR = Path(__file__).resolve().parent
_GENERATE_SUMMARY = _TESTS_DIR / "generate-summary.py"
_TEMPLATE_NAME = "summary.html"

# Names Jinja exposes implicitly (not passed from generate-summary.render).
_JINJA_IMPLICIT = frozenset({"caller", "kwargs"})


def _summary_html_render_kwargs() -> frozenset[str]:
    tree = ast.parse(_GENERATE_SUMMARY.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (
            isinstance(func, ast.Attribute)
            and func.attr == "render"
            and isinstance(func.value, ast.Call)
            and isinstance(func.value.func, ast.Attribute)
            and func.value.func.attr == "get_template"
        ):
            continue
        template_call = func.value
        if not template_call.args:
            continue
        template_name = template_call.args[0]
        if not isinstance(template_name, ast.Constant) or template_name.value != _TEMPLATE_NAME:
            continue
        return frozenset(kw.arg for kw in node.keywords if kw.arg is not None)
    raise AssertionError(f"{_TEMPLATE_NAME} .render() call not found in generate-summary.py")


class TestSummaryHtmlTemplate(unittest.TestCase):
    def test_undeclared_variables_are_render_kwargs(self) -> None:
        env = Environment(loader=FileSystemLoader(str(_TESTS_DIR / "templates")))
        source = env.loader.get_source(env, _TEMPLATE_NAME)[0]
        parsed = env.parse(source)
        undeclared = meta.find_undeclared_variables(parsed)
        render_kwargs = _summary_html_render_kwargs()
        missing = sorted(undeclared - render_kwargs - _JINJA_IMPLICIT)
        self.assertEqual(
            missing,
            [],
            f"{_TEMPLATE_NAME} references {missing!r} but render_testlist_html does not pass them",
        )


if __name__ == "__main__":
    unittest.main()
