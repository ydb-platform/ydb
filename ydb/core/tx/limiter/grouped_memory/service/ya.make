LIBRARY()

SRCS(
    actor.cpp
    manager.cpp
    group.cpp
    process.cpp
    allocation.cpp
    ids.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/library/signals
    ydb/core/tx/limiter/grouped_memory/tracing
    ydb/core/tx/limiter/grouped_memory/usage
)

GENERATE_ENUM_SERIALIZATION(allocation.h)

END()
