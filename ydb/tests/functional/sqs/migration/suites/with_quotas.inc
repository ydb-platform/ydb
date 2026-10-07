TEST_SRCS(
    ydb/tests/functional/sqs/with_quotas/test_quoting.py
)

DATA(
    arcadia/ydb/tests/functional/sqs/with_quotas
)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

IF (SANITIZER_TYPE == "thread")
    REQUIREMENTS(ram:32 cpu:1)
ELSE()
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
