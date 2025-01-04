LIBRARY()

SRCS(
    manager.cpp
    collector.cpp
    constructor.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/portions
    ydb/core/protos
)

END()
