PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_result_set_value.py
    test_result_set_arrow.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:16)
REQUIREMENTS(ram:16)
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
