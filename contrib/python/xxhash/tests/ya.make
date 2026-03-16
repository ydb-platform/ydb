PY3TEST()

PEERDIR(
    contrib/python/xxhash
)

TEST_SRCS(
    test.py
    test_algorithms_available.py
    test_name.py
    test_xxh32.py
    test_xxh3_128.py
    test_xxh3_64.py
    test_xxh64.py
)

NO_LINT()

END()
