LIBRARY()

SRCS(
    cache.cpp
    journal.cpp
)

PEERDIR(
    ydb/core/protos/hnsw
    ydb/core/base
    ydb/core/tx/datashard/hnsw_index
    ydb/library/actors/core
)

END()
