PY2TEST()

NO_LINT()

PEERDIR(
    contrib/python/sysv-ipc
)

TEST_SRCS(
    base.py
    test_message_queues.py
    test_semaphores.py
    test_memory.py
    test_module.py
)

END()
