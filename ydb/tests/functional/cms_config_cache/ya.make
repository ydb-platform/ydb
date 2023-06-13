PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
TEST_SRCS(
    main.py
)

TIMEOUT(600)

SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    ydb/tests/library
    ydb/public/sdk/python
)

FORK_SUBTESTS()

FORK_TEST_FILES()

END()
