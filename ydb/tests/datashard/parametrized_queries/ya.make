PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

FORK_SUBTESTS()
SPLIT_FACTOR(19)
SIZE(MEDIUM)

TEST_SRCS(
    test_parametrized_queries.py
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
