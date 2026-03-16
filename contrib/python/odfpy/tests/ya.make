PY3TEST()

NO_LINT()

PEERDIR(
    contrib/python/odfpy
    contrib/python/lxml
)

DATA(
    arcadia/contrib/python/odfpy/tests/examples
)

ALL_PYTEST_SRCS()

END()
