PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

FORK_SUBTESTS()
SPLIT_FACTOR(45)

SIZE(LARGE)
TAG(ya:fat)

TEST_SRCS(
    conftest.py
    test_split_merge_by_load.py
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/fixtures
)

DEPENDS(
    ydb/apps/ydb
)

END()
