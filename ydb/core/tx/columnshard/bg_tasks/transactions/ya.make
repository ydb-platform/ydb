LIBRARY()

SRCS(
    tx_general.cpp
    tx_save_progress.cpp
    tx_save_state.cpp
    tx_remove.cpp
    tx_add.cpp
)

PEERDIR(
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/bg_tasks/abstract
    ydb/core/tx/columnshard/bg_tasks/events
    ydb/core/tx/columnshard/bg_tasks/session
    ydb/core/protos
)

END()
