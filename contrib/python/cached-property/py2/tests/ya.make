PY2TEST()

PEERDIR(
    contrib/python/freezegun
    contrib/python/cached-property
)

TEST_SRCS(
    conftest.py
    test_cached_property.py
)

NO_LINT()

REQUIREMENTS(ram:14)

END()
