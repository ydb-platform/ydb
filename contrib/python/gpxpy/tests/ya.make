PY3TEST()

SRCDIR(contrib/python/gpxpy)

TEST_SRCS(
    test.py
)

DATA(
    arcadia/contrib/python/gpxpy/test_files
)

PEERDIR(
    contrib/python/gpxpy
)

NO_LINT()

END()
