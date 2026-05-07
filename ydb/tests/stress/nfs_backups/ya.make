PY3_PROGRAM(nfs_backups)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/stress/common
    ydb/tests/stress/nfs_backups/workload
)

END()

RECURSE_FOR_TESTS(
    tests
)
