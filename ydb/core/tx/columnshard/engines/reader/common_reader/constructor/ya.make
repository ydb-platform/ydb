LIBRARY()

SRCS(
    read_metadata.cpp
    read_metadata_owner.cpp  # gn: into ydb/core/tx/columnshard
    resolver.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/abstract
    ydb/core/kqp/compute_actor
    ydb/core/tx/columnshard/engines/reader/common
)

END()
