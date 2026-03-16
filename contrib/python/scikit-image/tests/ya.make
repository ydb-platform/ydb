PY3TEST()

SIZE(MEDIUM)

FORK_TESTS()

PEERDIR(
    contrib/python/scikit-image
    contrib/python/scikit-learn
    contrib/python/toolz
    contrib/python/dask
    contrib/python/pytest-localserver
    contrib/python/pytest-pretty
    contrib/python/matplotlib
)

DATA(
    arcadia/contrib/python/scikit-image/skimage
)

ALL_PYTEST_SRCS(RECURSIVE)

NO_LINT()

END()
