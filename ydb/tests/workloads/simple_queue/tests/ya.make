PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    test_workload.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:20)
ENDIF()

SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydbd
    ydb/apps/ydb
    ydb/tests/workloads/simple_queue
)

PEERDIR(
    ydb/tests/library
)


END()
