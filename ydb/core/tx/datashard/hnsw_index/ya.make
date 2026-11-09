LIBRARY()

ADDINCL(
    ydb/library/nmslib/include
)

SRCS(
    hnsw_index.h
    hnsw_index.cpp
    hnsw_index_build_actor.h
    hnsw_memory_tracker.h
    hnsw_memory_tracker.cpp
)

PEERDIR(
    ydb/core/base
    ydb/library/nmslib
    ydb/public/api/protos
)

END()
