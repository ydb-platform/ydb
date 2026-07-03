PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(32)

TEST_SRCS(
    test_result_set_value.py
    test_result_set_arrow.py
)

SIZE(LARGE)
IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32 cpu:8)
ELSE()
    REQUIREMENTS(cpu:4)
    REQUIREMENTS(ram:16)
ENDIF()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

DEPENDS(
    ydb/tests/library/compatibility/binaries
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/compatibility
    ydb/tests/datashard/lib
    contrib/python/pyarrow
)

END()
