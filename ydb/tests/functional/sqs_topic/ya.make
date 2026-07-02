PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_create_queue.py
    test_delete_queue.py
    test_send_message.py
    test_receive_message.py
)

ENV(YDB_USE_IN_MEMORY_PDISKS=true)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/sqs_topic
    contrib/python/boto3
    contrib/python/botocore
)

FORK_SUBTESTS()

END()
