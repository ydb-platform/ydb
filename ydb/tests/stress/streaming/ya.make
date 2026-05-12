PY3_PROGRAM(streaming)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/streaming/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)

