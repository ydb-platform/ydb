PY3_LIBRARY()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(MOTO_SERVER_PATH="contrib/python/moto/bin/moto_server")

PY_SRCS(
    base_async_replication.py
)

PEERDIR(
    ydb/tests/library
)

END()
