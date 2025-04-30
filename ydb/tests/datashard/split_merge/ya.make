PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")


FORK_SUBTESTS()
SPLIT_FACTOR(45)
SIZE(MEDIUM)


TEST_SRCS(
    test_split_merge.py

)

PEERDIR(
    ydb/tests/sql/lib
    ydb/tests/library
    ydb/tests/datashard/lib
)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
)

END()
