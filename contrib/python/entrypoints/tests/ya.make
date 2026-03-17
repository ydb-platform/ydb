PY3TEST()

PEERDIR(
    contrib/python/entrypoints
)

DATA(
    arcadia/contrib/python/entrypoints/py3/tests/samples
)

TEST_SRCS(
    __init__.py
    test_entrypoints.py
)

NO_LINT()

END()
