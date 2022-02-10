PY23_TEST()

OWNER(g:python-contrib)

PEERDIR(
    contrib/python/decorator
)

PY_SRCS(
    TOP_LEVEL
    documentation.py
)

TEST_SRCS(
    #documentation.py
    test.py
)

NO_LINT()

END()
