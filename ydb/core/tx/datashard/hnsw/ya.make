LIBRARY()

ADDINCL(ydb/library/nmslib/include)

SRCS(
    ../hnsw_index.cpp
    cache.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/scheme
    ydb/library/nmslib
    ydb/public/api/protos
)

END()
