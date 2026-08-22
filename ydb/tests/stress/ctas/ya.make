PY3_PROGRAM(ctas)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/ctas/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
