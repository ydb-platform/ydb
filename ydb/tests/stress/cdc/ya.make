PY3_PROGRAM(cdc)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/cdc/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
