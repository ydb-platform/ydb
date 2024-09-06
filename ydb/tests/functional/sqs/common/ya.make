PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(SQS_CLIENT_BINARY="ydb/core/ymq/client/bin/sqs")
ENV(YDB_USE_IN_MEMORY_PDISKS=true)

TEST_SRCS(
    test_account_actions.py
    test_acl.py
    test_counters.py
    test_garbage_collection.py
    test_multiplexing_tables_format.py
    test_ping.py
    test_queue_attributes_validation.py
    test_queues_managing.py
    test_format_without_version.py
    test_throttling.py
    test_queue_counters.py
)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(2400)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(
        ram:32
    )
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

DEPENDS(
    ydb/apps/ydbd
    ydb/core/ymq/client/bin
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/sqs
    ydb/tests/oss/ydb_sdk_import
    contrib/python/xmltodict
    contrib/python/boto3
    contrib/python/botocore
)

FORK_SUBTESTS()

# SQS tests are not CPU or disk intensive,
# but they use sleeping for some events,
# so it would be secure to increase split factor.
# This increasing of split factor reduces test time
# to 15-20 seconds.
SPLIT_FACTOR(60)

END()
