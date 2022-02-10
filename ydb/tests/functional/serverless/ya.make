OWNER(g:kikimr)
PY3TEST()

TEST_SRCS(
    conftest.py
    test.py
)

FORK_TEST_FILES()

TIMEOUT(600)

SIZE(MEDIUM)

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd") 
DEPENDS(ydb/apps/ydbd) 

PEERDIR(
    contrib/python/tornado/tornado-4
    ydb/tests/library
    ydb/public/sdk/python/ydb
)

END()
