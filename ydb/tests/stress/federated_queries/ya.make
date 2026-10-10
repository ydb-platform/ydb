PY3_PROGRAM(federated_queries)

STYLE_PYTHON()

PY_SRCS(
    __main__.py
)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/tests/stress/federated_queries/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
