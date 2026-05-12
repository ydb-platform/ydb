PY3_PROGRAM(topic_sqs)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/topic_sqs/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)

