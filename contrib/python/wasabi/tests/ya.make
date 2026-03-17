PY3TEST()

PEERDIR(
    contrib/python/typing-extensions
    contrib/python/wasabi
)

SRCDIR(contrib/python/wasabi/wasabi/tests)

TEST_SRCS(
    __init__.py
    test_markdown.py
    test_printer.py
    test_tables.py
    test_traceback.py
    test_util.py
)

NO_LINT()

END()
