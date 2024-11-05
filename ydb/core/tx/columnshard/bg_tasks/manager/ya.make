LIBRARY()

SRCS(
    manager.cpp
    actor.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/bg_tasks/abstract
    ydb/core/tx/columnshard/bg_tasks/protos
    ydb/core/tablet_flat
)

END()
