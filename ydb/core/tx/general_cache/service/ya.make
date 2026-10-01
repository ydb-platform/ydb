LIBRARY()

SRCS(
    manager.cpp
    counters.cpp
    service.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/base
    ydb/core/tx/general_cache/source
    ydb/core/tx/general_cache/usage
)

END()
