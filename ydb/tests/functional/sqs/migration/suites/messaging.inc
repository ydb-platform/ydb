TEST_SRCS(
    ydb/tests/functional/sqs/messaging/test_generic_messaging.py
    ydb/tests/functional/sqs/messaging/test_fifo_messaging.py
    ydb/tests/functional/sqs/messaging/test_polling.py
)

DATA(
    arcadia/ydb/tests/functional/sqs/messaging
)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:32 cpu:1)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ENDIF()

DEPENDS(
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/sqs
    contrib/python/xmltodict
    contrib/python/boto3
    contrib/python/botocore
)

FORK_SUBTESTS()
SPLIT_FACTOR(60)
