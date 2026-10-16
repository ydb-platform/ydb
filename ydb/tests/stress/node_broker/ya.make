PY3_PROGRAM(node_broker)

PY_SRCS(
    __main__.py
)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/node_broker/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
