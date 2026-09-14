PY3_PROGRAM(simple_queue)

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

