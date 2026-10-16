PY3_PROGRAM(transfer)

PY_SRCS(
    __main__.py
)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/tests/stress/transfer/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)

