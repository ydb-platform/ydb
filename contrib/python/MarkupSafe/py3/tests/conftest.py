from __future__ import annotations

import sys
import typing as t
from types import ModuleType

import pytest

import markupsafe
from markupsafe import _native

try:
    from markupsafe import _speedups
except ImportError:
    _speedups = None  # type: ignore


def pytest_report_header() -> list[str]:
    """Return a list of strings to be displayed in the header of the report."""
    if sys.version_info >= (3, 13):
        return [f"Free-threaded: {not sys._is_gil_enabled()}"]

    return []


@pytest.fixture(
    scope="session",
    autouse=True,
    params=(
        _native,
        pytest.param(
            _speedups,
            marks=pytest.mark.skipif(_speedups is None, reason="speedups unavailable"),
        ),
    ),
)
def _mod(request: pytest.FixtureRequest) -> None:
    mod = t.cast(ModuleType, request.param)
    markupsafe._escape_inner = mod._escape_inner  # type: ignore[attr-defined]
