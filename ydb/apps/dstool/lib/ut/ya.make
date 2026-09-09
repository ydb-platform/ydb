PY3TEST()

PEERDIR(
    ydb/apps/dstool/lib
    contrib/python/pytest
    contrib/python/grpcio
)

TEST_SRCS(test_grouptool.py)

END()
