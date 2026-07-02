TEST_SRCS(
    ydb/tests/functional/sqs/common/test_account_actions.py
    ydb/tests/functional/sqs/common/test_acl.py
    ydb/tests/functional/sqs/common/test_counters.py
    ydb/tests/functional/sqs/common/test_garbage_collection.py
    ydb/tests/functional/sqs/common/test_multiplexing_tables_format.py
    ydb/tests/functional/sqs/common/test_ping.py
    ydb/tests/functional/sqs/common/test_queue_attributes_validation.py
    ydb/tests/functional/sqs/common/test_queues_managing.py
    ydb/tests/functional/sqs/common/test_format_without_version.py
    ydb/tests/functional/sqs/common/test_throttling.py
    ydb/tests/functional/sqs/common/test_queue_counters.py
    ydb/tests/functional/sqs/common/test_queue_tags.py
)

DATA(
    arcadia/ydb/tests/functional/sqs/common
)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:32 cpu:4)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ENDIF()

DEPENDS(
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
SPLIT_FACTOR(60)
