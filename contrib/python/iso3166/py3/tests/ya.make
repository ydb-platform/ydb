PY3TEST()

PEERDIR(
    contrib/python/iso3166
)

TEST_SRCS(
    test_listings.py
    test_lookup.py
)

NO_LINT()

END()
