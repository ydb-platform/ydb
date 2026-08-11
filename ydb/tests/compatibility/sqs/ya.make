PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

PY_SRCS(
    ymq_rolling_base.py
)

TEST_SRCS(
    test_ymq_native.py
    test_ymq_boto.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:8)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

DEPENDS(
    ydb/tests/library/compatibility/binaries
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/compatibility
    ydb/tests/library/sqs
    contrib/python/boto3
)

END()
