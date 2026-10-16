PY3_PROGRAM(ctas)

PY_SRCS(
    __main__.py
)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/ctas/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
