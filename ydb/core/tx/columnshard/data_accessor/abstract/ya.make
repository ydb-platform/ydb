LIBRARY()

SRCS(
    manager.cpp
    collector.cpp
    constructor.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/engines/reader/tracing
    ydb/core/tx/columnshard/private_events
    ydb/core/protos
)

END()
