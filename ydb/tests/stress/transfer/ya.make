PY3_PROGRAM(transfer)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/simple_queue/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)

