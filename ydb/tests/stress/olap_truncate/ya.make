PY3_PROGRAM(olap_truncate)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/olap_truncate/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
