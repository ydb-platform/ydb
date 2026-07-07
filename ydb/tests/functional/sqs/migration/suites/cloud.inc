TEST_SRCS(
    ydb/tests/functional/sqs/cloud/test_common.py
    ydb/tests/functional/sqs/cloud/test_yandex_cloud_mode.py
    ydb/tests/functional/sqs/cloud/test_yandex_cloud_queue_counters.py
    ydb/tests/functional/sqs/cloud/test_yandex_audit.py
)

DATA(
    arcadia/ydb/tests/functional/sqs/cloud
)

REQUIREMENTS(cpu:2)
IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:32)
ELSE()
    SIZE(MEDIUM)
ENDIF()

DEPENDS(
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/sqs
    contrib/python/xmltodict
    contrib/python/botocore
)

FORK_SUBTESTS()
SPLIT_FACTOR(40)
