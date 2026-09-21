PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_topic.py
<<<<<<< HEAD
=======
    test_direct_read.py
    test_cluster_discovery.py
    test_sqs_topic_boto.py
>>>>>>> 33ea0ba5afe ([PQ] Hide FNX clusters in DiscoverClusters unless :authority matches a Balancer (LOGBROKER-10621) (#53259))
    test_kafka_topic.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:8)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

DEPENDS(
    ydb/tests/library/compatibility/binaries
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/compatibility
)

END()
