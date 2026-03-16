PY2TEST()

PEERDIR(
    contrib/python/ijson
)

TEST_SRCS(
    __init__.py
    test_base.py
    test_coroutines.py
    test_generators.py
    test_misc.py
)

NO_LINT()

END()
