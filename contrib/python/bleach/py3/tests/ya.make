PY3TEST()

PEERDIR(
    contrib/python/bleach
    contrib/python/tinycss2
)

DATA(
    arcadia/contrib/python/bleach/py3/tests/data
)

ALL_PYTEST_SRCS()

NO_LINT()

END()
