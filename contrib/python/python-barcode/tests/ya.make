PY3TEST()

PEERDIR(
    contrib/python/Pillow
    contrib/python/python-barcode
    contrib/python/pytest
)

NO_LINT()

ALL_PYTEST_SRCS(RECURSIVE)

END()
