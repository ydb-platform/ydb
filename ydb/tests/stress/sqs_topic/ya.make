PY3_PROGRAM(sqs_topic)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/sqs_topic/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)

