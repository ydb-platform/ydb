LIBRARY()

SRCS(
    manager.cpp
    collector.cpp
    GLOBAL constructor.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/data_accessor/abstract
)

END()
