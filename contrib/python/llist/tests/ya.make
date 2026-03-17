PY3TEST()

PEERDIR(
    contrib/python/llist
)

TEST_SRCS(
    dllist_test.py
    llist_test.py
    py23_utils.py
    sllist_test.py
    speed_test.py
)

NO_LINT()

END()
