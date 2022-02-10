OWNER(g:kikimr)
PY3TEST()

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd") 
TEST_SRCS(
    test_encryption.py
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

REQUIREMENTS(ram:11)

END()
