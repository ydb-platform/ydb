LIBRARY()

SRCS(
    read_metadata.cpp
    resolver.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/abstract
    ydb/core/kqp/compute_actor
    ydb/core/tx/columnshard/engines/reader/common
)

END()
