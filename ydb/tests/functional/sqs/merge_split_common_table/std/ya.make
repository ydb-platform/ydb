PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(SQS_CLIENT_BINARY="ydb/core/ymq/client/bin/sqs")

TEST_SRCS(
    test.py
)


IF (SANITIZER_TYPE == "thread")
    TIMEOUT(2400)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

REQUIREMENTS(
    ram:16
)

DEPENDS(
    ydb/apps/ydbd
    ydb/core/ymq/client/bin
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/sqs
    ydb/tests/functional/sqs/merge_split_common_table
    contrib/python/xmltodict
    contrib/python/boto3
    contrib/python/botocore
)

END()
