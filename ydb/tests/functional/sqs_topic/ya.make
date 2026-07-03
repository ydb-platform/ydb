PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_create_queue.py
    test_get_queue_url.py
    test_get_queue_attributes.py
    test_set_queue_attributes.py
    test_delete_queue.py
    test_send_message.py
    test_send_message_batch.py
    test_receive_message.py
    test_change_message_visibility.py
    test_change_message_visibility_batch.py
    test_delete_message.py
    test_delete_message_batch.py
    test_list_queues.py
    test_purge_queue.py
    test_add_permission.py
    test_remove_permission.py
    test_tag_queue.py
    test_untag_queue.py
    test_start_message_move_task.py
    test_cancel_message_move_task.py
    test_list_message_move_tasks.py
    test_list_dead_letter_source_queues.py
    test_list_queue_tags.py
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
