LIBRARY()

SRCS(
    ../hnsw.cpp
)

PEERDIR(
    ydb/core/scheme
    ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
