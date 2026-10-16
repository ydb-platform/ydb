PY3_PROGRAM(streaming)

STYLE_PYTHON()

PY_SRCS(
    __main__.py
)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/tests/stress/streaming/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)

