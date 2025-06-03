PY3_PROGRAM(s3_workload)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/s3_workload/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
