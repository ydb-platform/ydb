TEST_SRCS(
    ydb/tests/functional/sqs/multinode/test_multinode_cluster.py
    ydb/tests/functional/sqs/multinode/test_recompiles_requests.py
)

DATA(
    arcadia/ydb/tests/functional/sqs/multinode
)

REQUIREMENTS(cpu:4)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
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
