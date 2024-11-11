LIBRARY()

SRCS(
    manager.cpp
    collector.cpp
    constructor.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/data_accessor/local_db
    ydb/core/protos
)

END()
