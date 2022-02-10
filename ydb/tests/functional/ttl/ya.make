PY3TEST()

OWNER(
    ilnaz
    g:kikimr
)

TEST_SRCS(
    test_ttl.py
)

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
TIMEOUT(600)
SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    ydb/tests/library
    ydb/public/sdk/python/ydb
    contrib/python/PyHamcrest
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
