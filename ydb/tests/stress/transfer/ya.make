PY3_PROGRAM(transfer)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/transfer/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)

