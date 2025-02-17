IF (OS_LINUX)

PY3TEST()

TEST_SRCS(
    test_break.py
)

SIZE(MEDIUM)

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

PEERDIR(
    ydb/tests/library
)

DEPENDS(
    ydb/apps/ydbd
)


END()

ENDIF()
