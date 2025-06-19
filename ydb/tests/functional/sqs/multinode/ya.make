PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

TEST_SRCS(
    test_multinode_cluster.py
    test_recompiles_requests.py
)

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

# SQS tests are not CPU or disk intensive,
# but they use sleeping for some events,
# so it would be secure to increase split factor.
# This increasing of split factor reduces test time
# to 15-20 seconds.
SPLIT_FACTOR(60)

END()
