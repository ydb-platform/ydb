PY3TEST()

PEERDIR(
    contrib/python/annoy
    contrib/python/numpy
)

TEST_SRCS(
    test_index.py
)

END()
