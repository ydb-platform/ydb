LIBRARY()

SRCS(
    abstract.cpp
    events.cpp
    config.cpp
    service.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/general_cache/service
    ydb/core/tx/general_cache/source
)

END()
