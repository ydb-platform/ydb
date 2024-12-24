PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    test_sql.py
)

SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    ydb/tests/library
)

END()
