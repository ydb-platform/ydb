LIBRARY()

SRCS(
    events.cpp
    service.cpp
    common.cpp
)

PEERDIR(
    ydb/core/kqp/runtime
    ydb/core/tx/conveyor_composite/common
    ydb/core/tx/conveyor_composite/common/config
    ydb/library/actors/core
    ydb/services/metadata/request
    ydb/core/protos
    ydb/core/tx/conveyor/usage
)

END()
