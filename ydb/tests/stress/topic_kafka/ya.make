PY3_PROGRAM(workload_topic_kafka)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/topic_kafka/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
