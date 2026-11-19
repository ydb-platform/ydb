PY3TEST()

TEST_SRCS(test_legacy.py)
DEPENDS(ydb/tests/tools/parsed_query_bench)
PEERDIR(
    library/python/testing/yatest_common
    ydb/public/api/protos
)
END()
