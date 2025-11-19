PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

TEST_SRCS(
    test_generic_messaging.py
    test_fifo_messaging.py
    test_polling.py
)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:32 cpu:1)
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
