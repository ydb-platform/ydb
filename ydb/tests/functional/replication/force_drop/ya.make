PY3TEST()

SIZE(LARGE)
TIMEOUT(900)
FORK_SUBTESTS()

TEST_SRCS(test_force_drop.py)

PEERDIR(
    contrib/python/grpcio
    contrib/python/ydb/py3
    ydb/public/api/protos
    ydb/tests/library
)

DEPENDS(ydb/apps/ydbd)
ENV(YDB_DRIVER_BINARY=ydb/apps/ydbd/ydbd)

END()
