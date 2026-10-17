LIBRARY()

SRCS(
    manager.cpp  # gn: into ydb/core/tx/columnshard/data_accessor/abstract
    GLOBAL constructor.cpp  # gn: into ydb/core/tx/columnshard/data_accessor/abstract
)

PEERDIR(
    ydb/core/tx/columnshard/data_accessor/abstract
)

END()
