PY3_PROGRAM(kafka_streams_test)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/kafka/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
