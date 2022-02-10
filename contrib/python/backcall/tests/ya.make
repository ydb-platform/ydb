PY23_TEST()

OWNER(g:python-contrib)

PEERDIR(
    contrib/python/backcall
)

TEST_SRCS(
    test_callback_prototypes.py
)

NO_LINT()

END()
