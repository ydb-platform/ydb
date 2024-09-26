PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    test_compatibility.py
)

TIMEOUT(600)
SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydbd
    ydb/tests/library/compatibility
)

PEERDIR(
    ydb/tests/library
)

END()
