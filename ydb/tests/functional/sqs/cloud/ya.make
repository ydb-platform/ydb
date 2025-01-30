PY3TEST()

TEST_SRCS(
    test_common.py
    test_yandex_cloud_mode.py
    test_yandex_cloud_queue_counters.py
)

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:32 cpu:2)
ELSE()
    SIZE(MEDIUM)
ENDIF()

DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/sqs
    contrib/python/xmltodict
    contrib/python/boto3
    contrib/python/botocore
)

FORK_SUBTESTS()
SPLIT_FACTOR(40)

END()
