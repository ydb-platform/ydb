PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_USE_IN_MEMORY_PDISKS=true)

TEST_SRCS(
    test.py
)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
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
