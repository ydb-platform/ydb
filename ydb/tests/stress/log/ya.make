PY3_PROGRAM(workload_log)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/log/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)