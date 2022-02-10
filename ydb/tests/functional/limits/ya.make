OWNER(g:kikimr)

PY3TEST()

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd") 
TEST_SRCS(
    test_schemeshard_limits.py
)

TIMEOUT(600)
SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydbd 
)

PEERDIR(
    ydb/tests/library
    ydb/public/sdk/python/ydb
)

FORK_TEST_FILES()
FORK_SUBTESTS()

END()

