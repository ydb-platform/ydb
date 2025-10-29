PY3_PROGRAM(statistics_workload)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/statistics_workload/workload
)

END()
