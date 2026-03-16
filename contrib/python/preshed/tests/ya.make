PY3TEST()

PEERDIR(
    contrib/python/preshed
)

SRCDIR(contrib/python/preshed/preshed/tests)

TEST_SRCS(
    __init__.py
    test_bloom.py
    test_counter.py
    test_hashing.py
    test_pop.py
)

NO_LINT()

END()
