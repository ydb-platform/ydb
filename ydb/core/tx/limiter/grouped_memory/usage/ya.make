LIBRARY()

SRCS(
    events.cpp
    config.cpp
    abstract.cpp
    service.cpp
    stage_features.cpp
    counters.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/signals
    ydb/services/metadata/request
    ydb/core/base
    ydb/core/tx/limiter/grouped_memory/tracing
)

END()
