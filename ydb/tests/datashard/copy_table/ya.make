PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

FORK_SUBTESTS()
SPLIT_FACTOR(13)
SIZE(MEDIUM)



TEST_SRCS(
    test_copy_table.py
)

PEERDIR(
    ydb/tests/sql/lib
    ydb/tests/datashard/lib
)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
)

END()
