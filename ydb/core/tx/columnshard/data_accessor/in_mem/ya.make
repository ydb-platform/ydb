LIBRARY()

SRCS(
    manager.cpp
    GLOBAL constructor.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/data_accessor/abstract
)

END()
