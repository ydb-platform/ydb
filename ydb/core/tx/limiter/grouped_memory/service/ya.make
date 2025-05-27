LIBRARY()

SRCS(
    actor.cpp
    manager.cpp
    counters.cpp
    group.cpp
    process.cpp
    allocation.cpp
    ids.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/library/signals
)

GENERATE_ENUM_SERIALIZATION(allocation.h)

END()
