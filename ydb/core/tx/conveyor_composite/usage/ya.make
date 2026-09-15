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
    ydb/library/services
    ydb/services/metadata/request
    ydb/core/base
    ydb/core/protos
    ydb/core/tx/conveyor/usage
)

END()
