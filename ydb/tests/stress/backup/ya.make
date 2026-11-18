PY3_PROGRAM(backup_stress)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/backup/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)