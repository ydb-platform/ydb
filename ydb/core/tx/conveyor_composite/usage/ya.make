LIBRARY()

SRCS(
    events.cpp
    config.cpp
    service.cpp
    common.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/services/metadata/request
    ydb/core/protos
    ydb/core/tx/conveyor/usage
)

GENERATE_ENUM_SERIALIZATION(common.h)

END()
