PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
SIZE(MEDIUM)

REQUIREMENTS(cpu:4)

ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

TEST_SRCS(
    conftest.py
    test_canonical_requests.py
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/apps/dstool
    ydb/apps/dstool/lib
)

END()
