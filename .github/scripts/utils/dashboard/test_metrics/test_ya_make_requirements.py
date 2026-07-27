"""
Tests for ya_make_requirements: ya.make conditionals and effective split parsing.

Run: python3 test_ya_make_requirements.py
     or: pytest test_ya_make_requirements.py -v
"""

from __future__ import annotations

import importlib.util
import tempfile
import sys
from pathlib import Path

_MODULE_PATH = Path(__file__).resolve().parent / "ya_make_requirements.py"
_SPEC = importlib.util.spec_from_file_location("ya_make_requirements", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_ya_make_requirements = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _ya_make_requirements
_SPEC.loader.exec_module(_ya_make_requirements)

_parse_active_attrs = _ya_make_requirements._parse_active_attrs
get_requirements_for_suite = _ya_make_requirements.get_requirements_for_suite


def test_sanitizer_conditional_requirements():
    content = """PY3TEST()

IF (SANITIZER_TYPE == "thread")
    REQUIREMENTS(cpu:8 ram:64)
ELSE()
    REQUIREMENTS(cpu:2 ram:16)
ENDIF()

SIZE(LARGE)
END()
"""
    assert _parse_active_attrs(content, sanitizer="thread") == {
        "cpu_cores": 8,
        "ram_gb": 64,
        "size": "LARGE",
    }
    assert _parse_active_attrs(content, sanitizer=None) == {
        "cpu_cores": 2,
        "ram_gb": 16,
        "size": "LARGE",
    }


def test_fork_test_files_effective_split_counts_active_test_srcs_only():
    content = """PY3TEST()
FORK_TEST_FILES()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_a.py
    test_b.py
)

IF (SANITIZER_TYPE)
TEST_SRCS(
    test_san.py
)
ENDIF()

END()
"""
    attrs = _parse_active_attrs(content, sanitizer=None)
    assert attrs["split_factor"] == 10
    assert attrs["test_srcs_count"] == 2
    assert attrs["effective_split_factor"] == 20

    san_attrs = _parse_active_attrs(content, sanitizer="memory")
    assert san_attrs["split_factor"] == 10
    assert san_attrs["test_srcs_count"] == 3
    assert san_attrs["effective_split_factor"] == 30


def test_get_requirements_normalizes_partitioned_suite_path():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        suite = root / "ydb/tests/compatibility/indexes"
        suite.mkdir(parents=True)
        (suite / "ya.make").write_text(
            """PY3TEST()
REQUIREMENTS(cpu:4 ram:32)
SIZE(LARGE)
SPLIT_FACTOR(30)
END()
""",
            encoding="utf-8",
        )

        attrs = get_requirements_for_suite(root, "ydb/tests/compatibility/indexes/part7")

    assert attrs == {
        "cpu_cores": 4,
        "ram_gb": 32,
        "size": "LARGE",
        "split_factor": 30,
        "split_factor_tooltip": "SPLIT_FACTOR(30) from ya.make (no FORK_TEST_FILES).",
    }


if __name__ == "__main__":
    test_sanitizer_conditional_requirements()
    test_fork_test_files_effective_split_counts_active_test_srcs_only()
    test_get_requirements_normalizes_partitioned_suite_path()
    print("OK")
