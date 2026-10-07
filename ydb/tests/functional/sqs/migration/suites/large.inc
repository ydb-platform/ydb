TEST_SRCS(
    ydb/tests/functional/sqs/large/test_leader_start_inflight.py
)

DATA(
    arcadia/ydb/tests/functional/sqs/large
)

REQUIREMENTS(cpu:4)
SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32)
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
