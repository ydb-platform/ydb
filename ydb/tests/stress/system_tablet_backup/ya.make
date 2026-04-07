PY3_PROGRAM(system_tablet_backup)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/system_tablet_backup/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
