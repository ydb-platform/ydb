PY3_LIBRARY()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

REQUIREMENTS(ram:16)
ENV(TIMEOUT=1800)

PY_SRCS(
    test_base.py
)

PEERDIR(
    ydb/tests/library
    ydb/tests/olap/load/lib
)

END()
