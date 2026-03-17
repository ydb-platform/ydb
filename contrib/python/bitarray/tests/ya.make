PY3TEST()

PEERDIR(
    contrib/python/bitarray
)

DATA(
    arcadia/contrib/python/bitarray/bitarray
)

SRCDIR(
    contrib/python/bitarray/bitarray
)

TEST_SRCS(
    test_bitarray.py
    test_util.py
)

NO_LINT()

END()
