PY3_PROGRAM(cdc)

PY_SRCS(
    __main__.py
)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/cdc/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
