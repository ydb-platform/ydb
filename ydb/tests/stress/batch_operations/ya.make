PY3_PROGRAM(batch_operations)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/batch_operations/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
