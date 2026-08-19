PY3TEST()

SIZE(MEDIUM)
TIMEOUT(300)

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
    ydb/public/tools/local_ydb
)

PEERDIR(
    library/python/port_manager
    ydb/tests/library
)

DATA(
    arcadia/.github/docker/files/health_check
    arcadia/.github/docker/files/initialize_local_ydb
)

TEST_SRCS(
    test_local_ydb_entrypoint.py
)

END()
