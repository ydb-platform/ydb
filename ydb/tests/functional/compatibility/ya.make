PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    test_compatibility.py
)

TIMEOUT(3600)
SIZE(LARGE)
TAG(ya:fat)

DEPENDS(
    ydb/apps/ydbd
    ydb/tests/library/compatibility
)

PEERDIR(
    ydb/tests/library
)

END()
