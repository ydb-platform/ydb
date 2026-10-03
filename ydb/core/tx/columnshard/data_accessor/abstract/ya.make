LIBRARY()

SRCS(
    manager.cpp
    collector.cpp
    constructor.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/data_accessor/abstract/interface
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/engines/reader/tracing
    ydb/core/protos
)

END()
