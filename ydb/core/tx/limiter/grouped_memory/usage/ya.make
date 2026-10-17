LIBRARY()

SRCS(
    events.cpp  # gn: into ydb/core/tx/limiter/grouped_memory/service
    config.cpp  # gn: into ydb/core/tx/limiter/grouped_memory/service
    abstract.cpp  # gn: into ydb/core/tx/limiter/grouped_memory/service
    service.cpp  # gn: into ydb/core/tx/limiter/grouped_memory/service
    stage_features.cpp  # gn: into ydb/core/tx/limiter/grouped_memory/service
)

PEERDIR(
    ydb/library/actors/core
    ydb/services/metadata/request
    ydb/core/tx/limiter/grouped_memory/service
)

END()
