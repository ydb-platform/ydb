PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_ERASURE=mirror_3_dc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_TEST_PATH="ydb/tests/stress/sqs_topic/sqs_topic")

TEST_SRCS(
    test_workload.py
    test_boto_stress.py
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(LARGE)
TAG(ya:fat)

DEPENDS(
    ydb/apps/ydb
    ydb/tests/stress/sqs_topic
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/tests/library/stress
    ydb/tests/stress/sqs_topic/workload
    contrib/python/boto3
    contrib/python/botocore
)

END()
