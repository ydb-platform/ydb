PY3_PROGRAM(workload_topic)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/topic/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)