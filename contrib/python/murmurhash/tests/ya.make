PY3TEST()

PEERDIR(
    contrib/python/murmurhash
)

SRCDIR(contrib/python/murmurhash/murmurhash/tests)

TEST_SRCS(
    __init__.py
    test_hash.py
    test_import.py
)

NO_LINT()

END()
