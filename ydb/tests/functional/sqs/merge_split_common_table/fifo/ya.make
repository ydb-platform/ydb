PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    test.py
)


IF (SANITIZER_TYPE)
    TIMEOUT(2400)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(
        cpu:4
        ram:32
    )
ELSE()
    REQUIREMENTS(
        cpu:4
        ram:16
    )
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
    ydb/tests/functional/sqs/merge_split_common_table
    contrib/python/xmltodict
    contrib/python/boto3
    contrib/python/botocore
)

END()
