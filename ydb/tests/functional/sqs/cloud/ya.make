PY3TEST()

TEST_SRCS(
    test_common.py
    test_yandex_cloud_mode.py
    test_yandex_cloud_queue_counters.py
    test_yandex_audit.py
)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

REQUIREMENTS(cpu:2)
IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:32)
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
SPLIT_FACTOR(40)

END()
