PY3_PROGRAM(kafka_streams_test)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/kafka/workload
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
)

END()

RECURSE_FOR_TESTS(
    tests
)
