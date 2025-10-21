PY3_PROGRAM(show_create_view)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/show_create/view/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
