LIBRARY()

SRCS(
    constructor.cpp
    resolver.cpp
    read_metadata.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/abstract
    ydb/core/kqp/compute_actor
)

END()
