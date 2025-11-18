PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_WORKLOAD_PATH="ydb/tests/stress/kafka/kafka_streams_test")

TEST_SRCS(
    test_kafka_streams.py
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(LARGE)

DEPENDS(
    ydb/apps/ydb
    ydb/tests/stress/kafka
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
    ydb/tests/stress/kafka/workload
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
)

REQUIREMENTS(network:full)

END()
