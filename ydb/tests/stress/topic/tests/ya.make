PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_WORKLOAD_PATH="ydb/tests/stress/topic/workload_topic")

TEST_SRCS(
    test_workload_topic.py
)

SIZE(MEDIUM)
REQUIREMENTS(ram:32 cpu:4)

IF (SANITIZER_TYPE == "memory")
    ENV(YDB_STRESS_TEST_LIMIT_MEMORY=1)
ENDIF()


DEPENDS(
    ydb/apps/ydb
    ydb/tests/stress/topic
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
)


END()
