PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

FORK_SUBTESTS()
SPLIT_FACTOR(18)

SIZE(MEDIUM)

TEST_SRCS(
    test_dump_restore.py
)

PEERDIR(
    ydb/tests/datashard/lib
    ydb/tests/sql/lib
)

DEPENDS(
    ydb/apps/ydb
)

END()
