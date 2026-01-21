PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_rename_table.py
    test_compression.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:16)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)


DEPENDS(
    ydb/tests/library/compatibility/binaries
)

PEERDIR(
    contrib/python/boto3
    ydb/tests/library
    ydb/tests/library/compatibility
)

END()
