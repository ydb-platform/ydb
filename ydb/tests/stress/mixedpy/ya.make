PY3_PROGRAM(workload_mixed)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/mixedpy/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)