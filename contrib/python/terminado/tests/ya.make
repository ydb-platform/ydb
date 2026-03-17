PY3TEST()

PEERDIR(
    contrib/python/terminado
)

NO_LINT()

TEST_SRCS(
    __init__.py
    basic_test.py
)

REQUIREMENTS(ram:12)

END()
