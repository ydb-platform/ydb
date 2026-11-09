LIBRARY()

ADDINCL(
    ydb/library/nmslib/include
)

SRCS(
    datashard_hnsw.cpp
    hnsw_index.h
    hnsw_index.cpp
    hnsw_index_build_actor.h
    hnsw_index_build_actor.cpp
    hnsw_memory_tracker.h
)

PEERDIR(
    ydb/core/base
    ydb/library/nmslib
    ydb/public/api/protos
)

END()
