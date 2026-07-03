PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_datetime2.py
    test_digest.py
    test_digest_regression.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:4)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

DEPENDS(
    ydb/tests/library/compatibility/binaries
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/compatibility
)

END()
