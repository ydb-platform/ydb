PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    test_followers.py
    test_compatibility.py
    test_column_family.py
)

SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydbd
    ydb/tests/library/compatibility
)

PEERDIR(
    ydb/tests/library
)

END()
