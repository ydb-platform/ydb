PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_WORKLOAD_PATH="ydb/tests/stress/topic_kafka/workload_topic_kafka")

TEST_SRCS(
    test_workload_topic.py
)

SIZE(LARGE)
REQUIREMENTS(ram:32 cpu:4)

DEPENDS(
    ydb/apps/ydb
    ydb/tests/stress/topic_kafka
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
)


END()
