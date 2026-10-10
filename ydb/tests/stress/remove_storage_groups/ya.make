PY3_PROGRAM(remove_storage_groups)

PY_SRCS(
    __main__.py
)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/tests/stress/remove_storage_groups/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
