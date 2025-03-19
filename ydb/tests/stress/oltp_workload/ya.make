PY3_PROGRAM(oltp_workload)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/oltp_workload/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
