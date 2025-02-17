PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    test_example.py  # TODO: change file name to yours
)

SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    ydb/tests/library
)

END()
