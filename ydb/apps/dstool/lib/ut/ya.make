PY3TEST()

SIZE(SMALL)

TEST_SRCS(
    test_pdisk_location_filter.py
)

PEERDIR(
    contrib/python/PyYAML
    contrib/python/pytest
    ydb/apps/dstool/lib
)

END()
