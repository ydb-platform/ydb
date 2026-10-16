PY3_PROGRAM(show_create_table)

PY_SRCS(
    __main__.py
)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/show_create/table/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)

