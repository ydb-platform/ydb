PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(test.py)

SIZE(MEDIUM)
DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    contrib/python/requests
    contrib/python/urllib3
    ydb/tests/library
    ydb/public/sdk/python/enable_v3_new_behavior
)

END()
