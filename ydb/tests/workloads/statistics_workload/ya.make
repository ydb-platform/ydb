PY3_PROGRAM(statistics_workload)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/library
    ydb/public/sdk/python
    library/python/monlib
)

END()
