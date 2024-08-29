PY3_PROGRAM(olap_workload_tiering)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/public/sdk/python
    library/python/monlib
)

END()
