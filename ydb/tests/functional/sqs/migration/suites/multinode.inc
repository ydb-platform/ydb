SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

TEST_SRCS(
    ydb/tests/functional/sqs/multinode/test_multinode_cluster.py
    ydb/tests/functional/sqs/multinode/test_recompiles_requests.py
)

DATA(
    arcadia/ydb/tests/functional/sqs/multinode
)

REQUIREMENTS(cpu:4)

DEPENDS(
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/sqs
    contrib/python/xmltodict
    contrib/python/boto3
    contrib/python/botocore
)
