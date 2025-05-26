PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

FORK_SUBTESTS()
SPLIT_FACTOR(45)

SIZE(MEDIUM)

TEST_SRCS(
    test_select.py

)

PEERDIR(
    ydb/tests/datashard/lib
    ydb/tests/sql/lib
)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
)

END()
