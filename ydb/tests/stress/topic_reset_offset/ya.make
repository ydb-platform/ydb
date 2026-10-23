PY3_PROGRAM(topic_reset_offset)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/topic_reset_offset/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
