PY3_PROGRAM(workload_fulltext)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/fulltext_workload/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
