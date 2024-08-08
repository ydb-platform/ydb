PY3TEST()

TEST_SRCS(
    test_common.py
    test_yandex_cloud_mode.py
    test_yandex_cloud_queue_counters.py
)

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(SQS_CLIENT_BINARY="ydb/core/ymq/client/bin/sqs")

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(2400)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(
        cpu:1
        ram:32
    )
ELSE()
    REQUIREMENTS(
        cpu:1
        ram:16
    )
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
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
SPLIT_FACTOR(40)

END()
