PY3TEST()

PEERDIR(
    contrib/python/python-geohash
)

TEST_SRCS(
    test_geohash.py
    test_jpgrid.py
    test_jpiarea.py
    test_uint64.py
)

NO_LINT()

END()
