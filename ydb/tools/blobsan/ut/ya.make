PY3TEST()

TEST_SRCS(
    test_blobsan.py
)

DEPENDS(
    ydb/tools/blobsan
)

PEERDIR(
    library/python/testing/yatest_common
)

END()
