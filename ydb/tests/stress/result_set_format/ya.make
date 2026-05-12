PY3_PROGRAM(result_set_format)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/result_set_format/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
