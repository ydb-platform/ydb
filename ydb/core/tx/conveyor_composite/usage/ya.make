LIBRARY()

SRCS(
    events.cpp
    config.cpp
    service.cpp
    common.cpp
)

PEERDIR(
    ydb/core/tx/conveyor_composite/common
    ydb/library/actors/core
    ydb/services/metadata/request
    ydb/core/protos
    ydb/core/tx/conveyor/usage
)

END()
