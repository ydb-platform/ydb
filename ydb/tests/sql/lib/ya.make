PY3_LIBRARY()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

PY_SRCS(
    test_base.py
    test_lib.py
    test_query.py
)

PEERDIR(
    ydb/tests/library
)

END()
