PY3TEST()

PEERDIR(
    contrib/python/geoindex
)

TEST_SRCS(
    test_geo_point.py
    test_index_accurate.py
    test_performance.py
)

NO_LINT()

END()
