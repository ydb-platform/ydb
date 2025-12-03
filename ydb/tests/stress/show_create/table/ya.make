PY3_PROGRAM(show_create_table)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/show_create/table/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)

