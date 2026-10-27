LIBRARY()

ADDINCL(
    ydb/library/nmslib/include
)

SRCS(
    ../hnsw_index.cpp
)

PEERDIR(
    ydb/core/base
    ydb/library/nmslib
    ydb/public/api/protos
)

END()
