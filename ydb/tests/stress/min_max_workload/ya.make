PY3_PROGRAM(min_max_workload)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/min_max_workload/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
