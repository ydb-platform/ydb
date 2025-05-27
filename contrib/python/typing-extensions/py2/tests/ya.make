PY2TEST()

PEERDIR(
    contrib/python/typing-extensions
)

SRCDIR(
    contrib/python/typing-extensions/py2/src_py2
)

TEST_SRCS(
    test_typing_extensions.py
)

NO_LINT()

END()
