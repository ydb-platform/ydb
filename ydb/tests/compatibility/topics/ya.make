PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(16)

TEST_SRCS(
    test_topic.py
    test_direct_read.py
    test_sqs_topic.py
    test_sqs_topic_boto.py
    test_kafka_topic.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:8 network:full)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

DEPENDS(
    ydb/tests/library/compatibility/binaries
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/compatibility
    ydb/tests/stress/sqs_topic/workload
    contrib/python/boto3
)

END()
