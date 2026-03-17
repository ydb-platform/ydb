PY3TEST()

PEERDIR(
    contrib/python/trollsift
)

NO_LINT()

SRCDIR(contrib/python/trollsift/trollsift/tests)

TEST_SRCS(
    __init__.py
    integrationtests/__init__.py
    integrationtests/test_parser.py
    regressiontests/__init__.py
    regressiontests/test_parser.py
    unittests/__init__.py
    unittests/test_parser.py
)

END()
