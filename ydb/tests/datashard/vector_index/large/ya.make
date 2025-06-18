PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

SIZE(LARGE)
TAG(ya:fat)

FORK_SUBTESTS()
SPLIT_FACTOR(2)

TEST_SRCS(
    test_vector_index_large_levels_and_clusters.py
    test_vector_index.py
)

PEERDIR(
    ydb/tests/datashard/lib
)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
)

END()
