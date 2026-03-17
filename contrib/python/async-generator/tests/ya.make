PY3TEST()

NO_LINT()

SRCDIR(contrib/python/async-generator/async_generator/_tests)

PEERDIR(
    contrib/python/async-generator
)

TEST_SRCS(
    __init__.py
    conftest.py
    test_async_generator.py
    test_util.py
)

END()
