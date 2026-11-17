PY3_PROGRAM(snapshot_retention)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/python/pytest
    ydb/tests/library/snapshot_retention/workload
)

END()
