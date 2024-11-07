LIBRARY()

SRCS(
    manager.cpp
    collector.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/data_accessor/abstract
)

END()
