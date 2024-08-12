PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(SQS_CLIENT_BINARY="ydb/core/ymq/client/bin/sqs")

TEST_SRCS(
    test_multinode_cluster.py
    test_recompiles_requests.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16)
ENDIF()

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(2400)
    SIZE(LARGE)
    TAG(ya:fat)
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
