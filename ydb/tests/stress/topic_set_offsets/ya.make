PY3_PROGRAM(topic_set_offsets)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/topic_set_offsets/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
