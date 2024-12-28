PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    test.py
)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:32 cpu:4)
ELSE()
    SIZE(MEDIUM)
ENDIF()

DEPENDS(
    ydb/apps/ydbd
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
