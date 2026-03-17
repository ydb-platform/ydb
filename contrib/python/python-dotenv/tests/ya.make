PY3TEST()

PEERDIR(
    contrib/python/click
    contrib/python/python-dotenv
    contrib/python/sh
)

TEST_SRCS(
    __init__.py
    conftest.py
    # test_cli.py
    test_ipython.py
    test_main.py
    test_parser.py
    test_utils.py
    test_variables.py
)

END()
