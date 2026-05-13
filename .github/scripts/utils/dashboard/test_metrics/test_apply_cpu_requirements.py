"""
Tests for apply_cpu_requirements: apply CPU REQUIREMENTS to ya.make content.

Test data and expected results are defined in APPLY_CPU_TEST_CASES.

Run: python3 test_apply_cpu_requirements.py
     or: pytest test_apply_cpu_requirements.py -v
"""

from __future__ import annotations

import sys

from apply_cpu_requirements import apply_cpu_requirements_to_content

try:
    import pytest
except ImportError:
    pytest = None


# --- Test data: (content, cpu, sanitizer) -> (expected_content, expected_status) ---

# Inputs
YA_MAKE_WITH_VALGRIND_ELSE_ONLY = """UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(5)

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ydb_index_ut.cpp
)

END()
"""

YA_MAKE_WITH_VALGRIND_NO_REQ = """UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    foo.cpp
)
END()
"""

YA_MAKE_PLAIN_WITH_END = """UNITTEST_FOR(ydb/core/foo)

FORK_SUBTESTS()
SIZE(MEDIUM)

SRCS(
    foo.cpp
)
END()
"""

YA_MAKE_NO_END = """UNITTEST_FOR(ydb/core/foo)

SRCS(
    foo.cpp
)
"""

YA_MAKE_ALREADY_TOP_REQ = """UNITTEST_FOR(ydb/core/foo)

FORK_SUBTESTS()
REQUIREMENTS(cpu:2)

IF (WITH_VALGRIND)
    SIZE(LARGE)
ELSE()
    SIZE(MEDIUM)
ENDIF()
END()
"""

YA_MAKE_SANITIZER_BLOCK = """UNITTEST_FOR(ydb/core/foo)

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

SRCS(foo.cpp)
END()
"""

# Expected outputs
EXPECTED_DEFAULT_INSERT_AT_TOP = """UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(5)

REQUIREMENTS(cpu:2)
IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ydb_index_ut.cpp
)

END()
"""

EXPECTED_SANITIZER_VALGRIND_ELSEIF = """UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(cpu:2)
ELSEIF(SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    foo.cpp
)
END()
"""

EXPECTED_PLAIN_INSERT = """UNITTEST_FOR(ydb/core/foo)

FORK_SUBTESTS()
SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

SRCS(
    foo.cpp
)
END()
"""

EXPECTED_UPDATE_EXISTING = """UNITTEST_FOR(ydb/core/foo)

FORK_SUBTESTS()
REQUIREMENTS(cpu:4)

IF (WITH_VALGRIND)
    SIZE(LARGE)
ELSE()
    SIZE(MEDIUM)
ENDIF()
END()
"""

EXPECTED_SANITIZER_UPDATE = """UNITTEST_FOR(ydb/core/foo)

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:4)
ENDIF()

SRCS(foo.cpp)
END()
"""


APPLY_CPU_TEST_CASES = [
    # (name, content, cpu, sanitizer, expected_content, expected_status)
    (
        "default_insert_at_top",
        YA_MAKE_WITH_VALGRIND_ELSE_ONLY,
        "2",
        None,
        EXPECTED_DEFAULT_INSERT_AT_TOP,
        "updated",
    ),
    (
        "sanitizer_valgrind_elseif",
        YA_MAKE_WITH_VALGRIND_NO_REQ,
        "2",
        "thread",
        EXPECTED_SANITIZER_VALGRIND_ELSEIF,
        "updated",
    ),
    (
        "plain_with_end_insert_default",
        YA_MAKE_PLAIN_WITH_END,
        "2",
        None,
        EXPECTED_PLAIN_INSERT,
        "updated",
    ),
    (
        "no_module_block_skip",
        YA_MAKE_NO_END,
        "2",
        None,
        YA_MAKE_NO_END,
        "skip (no module block)",
    ),
    (
        "update_existing_top_requirement",
        YA_MAKE_ALREADY_TOP_REQ,
        "4",
        None,
        EXPECTED_UPDATE_EXISTING,
        "updated",
    ),
    (
        "same_value_no_change",
        YA_MAKE_ALREADY_TOP_REQ,
        "2",
        None,
        YA_MAKE_ALREADY_TOP_REQ,
        "no change",
    ),
    (
        "update_sanitizer_block",
        YA_MAKE_SANITIZER_BLOCK,
        "4",
        "thread",
        EXPECTED_SANITIZER_UPDATE,
        "updated",
    ),
]


def _run_one(name: str, content: str, cpu: str, sanitizer: str | None, expected_content: str, expected_status: str) -> bool:
    """Run one test case. Return True if passed."""
    result_content, result_status = apply_cpu_requirements_to_content(content, cpu, sanitizer)
    ok = result_status == expected_status and result_content == expected_content
    if not ok:
        if result_status != expected_status:
            print(f"  FAIL {name}: status got {result_status!r}, expected {expected_status!r}")
        if result_content != expected_content:
            print(f"  FAIL {name}: content mismatch")
            print(f"    expected ({len(expected_content)} chars): ...")
            print(f"    got ({len(result_content)} chars): ...")
    return ok


def test_apply_cpu_requirements_etalon(
    name: str,
    content: str,
    cpu: str,
    sanitizer: str | None,
    expected_content: str,
    expected_status: str,
) -> None:
    """On test data, apply_cpu_requirements_to_content yields expected content and status."""
    result_content, result_status = apply_cpu_requirements_to_content(content, cpu, sanitizer)
    assert result_status == expected_status, f"status: got {result_status!r}"
    assert result_content == expected_content, (
        "content mismatch:\n"
        f"--- expected ---\n{expected_content!r}\n"
        f"--- got ---\n{result_content!r}"
    )


def run_all_cases() -> int:
    """Run all APPLY_CPU_TEST_CASES. Return number of failures."""
    failed = 0
    for name, content, cpu, sanitizer, expected_content, expected_status in APPLY_CPU_TEST_CASES:
        if not _run_one(name, content, cpu, sanitizer, expected_content, expected_status):
            failed += 1
    return failed


if pytest is not None:
    @pytest.mark.parametrize(
        "name,content,cpu,sanitizer,expected_content,expected_status",
        APPLY_CPU_TEST_CASES,
        ids=[c[0] for c in APPLY_CPU_TEST_CASES],
    )
    def test_apply_cpu_requirements_parametrized(
        name: str,
        content: str,
        cpu: str,
        sanitizer: str | None,
        expected_content: str,
        expected_status: str,
    ) -> None:
        """Parametrized pytest: on test data we get expected content and status."""
        test_apply_cpu_requirements_etalon(
            name, content, cpu, sanitizer, expected_content, expected_status
        )


if __name__ == "__main__":
    print("Running apply_cpu_requirements tests...")
    n = run_all_cases()
    if n == 0:
        print("All tests passed.")
        sys.exit(0)
    print(f"Failed: {n}")
    sys.exit(1)
