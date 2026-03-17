PY3TEST()

PEERDIR(
    contrib/python/queuelib
)

SRCDIR(
    contrib/python/queuelib/py3/queuelib/tests
)

PY_SRCS(
    NAMESPACE queuelib.tests
    __init__.py
)

TEST_SRCS(
    test_pqueue.py
    test_queue.py
    test_rrqueue.py
)

NO_LINT()

END()
