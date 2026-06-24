LIBRARY()

SRCS(
    read_metadata.cpp
    resolver.cpp
)

PEERDIR(
    ydb/core/kqp/compute_actor/events
    ydb/core/tx/columnshard/engines/reader/abstract
)

END()
