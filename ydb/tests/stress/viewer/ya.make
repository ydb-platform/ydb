PY3_PROGRAM(viewer)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/viewer/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
