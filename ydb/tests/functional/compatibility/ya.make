PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    test_followers.py
    test_compatibility.py
)

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
