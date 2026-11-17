SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

TEST_SRCS(
    ydb/tests/functional/sqs/with_quotas/test_quoting.py
)

DATA(
    arcadia/ydb/tests/functional/sqs/with_quotas
)

REQUIREMENTS(cpu:1)
IF (SANITIZER_TYPE == "thread")
    REQUIREMENTS(ram:32)
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
