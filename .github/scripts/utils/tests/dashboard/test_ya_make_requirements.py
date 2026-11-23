"""
Tests for ya_make_requirements: ya.make conditionals and effective split parsing.

Run: python3 -m unittest discover -s .github/scripts/utils/tests -p 'test_*.py'
"""

from __future__ import annotations

import importlib.util
import tempfile
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _paths import TEST_METRICS

_MODULE_PATH = TEST_METRICS / "ya_make_requirements.py"
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


def test_multiline_requirements_block():
    content = """UNITTEST_FOR(ydb/core/mind)

IF (SANITIZER_TYPE  == "thread")
    SIZE(LARGE)
    REQUIREMENTS(
        ram:32
    )
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
"""
    assert _parse_active_attrs(content, sanitizer="thread") == {
        "ram_gb": 32,
        "size": "LARGE",
    }
    assert _parse_active_attrs(content, sanitizer=None) == {"size": "MEDIUM"}


def test_elseif_sanitizer_branch():
    content = """UNITTEST()

IF (WITH_VALGRIND)
    REQUIREMENTS(cpu:1 ram:8)
ELSEIF(SANITIZER_TYPE)
    REQUIREMENTS(cpu:4 ram:16)
ELSE()
    REQUIREMENTS(cpu:2 ram:8)
ENDIF()

END()
"""
    assert _parse_active_attrs(content, sanitizer=None) == {"cpu_cores": 2, "ram_gb": 8}
    assert _parse_active_attrs(content, sanitizer="address") == {"cpu_cores": 4, "ram_gb": 16}


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


def test_cpu_all_sentinel_is_preserved():
    content = """UNITTEST()
REQUIREMENTS(cpu:all ram:32)
SIZE(LARGE)
END()
"""
    assert _parse_active_attrs(content, sanitizer=None) == {
        "cpu_cores": "all",
        "ram_gb": 32,
        "size": "LARGE",
    }


def test_opensource_condition_is_active():
    content = """UNITTEST()
IF (OPENSOURCE)
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:4)
ELSE()
    SIZE(LARGE)
    REQUIREMENTS(cpu:16)
ENDIF()
END()
"""
    assert _parse_active_attrs(content, sanitizer=None) == {"cpu_cores": 4, "size": "MEDIUM"}


def test_inline_comment_does_not_stick_the_if_stack():
    content = """UNITTEST()
IF (SANITIZER_TYPE == "address")  # asan only
    REQUIREMENTS(cpu:8)
ENDIF() # IF (SANITIZER_TYPE == "address")
REQUIREMENTS(ram:4)
SIZE(SMALL)
END()
"""
    plain = _parse_active_attrs(content, sanitizer=None)
    assert plain.get("cpu_cores") is None
    assert plain["ram_gb"] == 4
    assert plain["size"] == "SMALL"
    asan = _parse_active_attrs(content, sanitizer="address")
    assert asan["cpu_cores"] == 8
    assert asan["ram_gb"] == 4


def test_unknown_condition_name_is_not_treated_as_sanitizer_match():
    content = """UNITTEST()
IF (HOST_OS_LINUX AND SANITIZER_TYPE == "address" AND NOT OPENSOURCE)
    REQUIREMENTS(cpu:32)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()
END()
"""
    assert _parse_active_attrs(content, sanitizer="address")["cpu_cores"] == 2


def test_commented_out_test_srcs_are_not_counted():
    content = """PY3TEST()
FORK_TEST_FILES()
SPLIT_FACTOR(5)
TEST_SRCS(
    test_a.py
    # test_disabled.py
    test_b.py
    #test_also_disabled.py
)
END()
"""
    attrs = _parse_active_attrs(content, sanitizer=None)
    assert attrs["test_srcs_count"] == 2
    assert attrs["effective_split_factor"] == 10


_ALL_TESTS = (
    test_sanitizer_conditional_requirements,
    test_fork_test_files_effective_split_counts_active_test_srcs_only,
    test_multiline_requirements_block,
    test_elseif_sanitizer_branch,
    test_get_requirements_normalizes_partitioned_suite_path,
    test_cpu_all_sentinel_is_preserved,
    test_opensource_condition_is_active,
    test_inline_comment_does_not_stick_the_if_stack,
    test_unknown_condition_name_is_not_treated_as_sanitizer_match,
    test_commented_out_test_srcs_are_not_counted,
)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for fn in _ALL_TESTS:
        suite.addTest(unittest.FunctionTestCase(fn))
    return suite


if __name__ == "__main__":
    for fn in _ALL_TESTS:
        fn()
    print("OK")
