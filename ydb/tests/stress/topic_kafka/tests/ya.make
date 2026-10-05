PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_WORKLOAD_PATH="ydb/tests/stress/topic_kafka/workload_topic_kafka")

TEST_SRCS(
    test_workload_topic.py
)

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32 cpu:2)
ELSE()
    REQUIREMENTS(ram:32 cpu:2)
ENDIF()

DEPENDS(
    ydb/apps/ydb
    ydb/tests/stress/topic_kafka
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
)


END()
