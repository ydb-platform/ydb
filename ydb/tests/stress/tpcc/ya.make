PY3_PROGRAM(workload_tpcc)

PY_SRCS(__main__.py)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/tpcc/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
