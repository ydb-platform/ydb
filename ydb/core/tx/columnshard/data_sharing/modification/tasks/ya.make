LIBRARY()

# gn: move into parent

SRCS(
    modification.cpp
    modification_tx.cpp  # gn: into ydb/core/tx/columnshard
)

PEERDIR(
    ydb/core/tx/columnshard/data_sharing/initiator/controller
    ydb/core/tx/columnshard/data_sharing/common/session
    ydb/core/tx/columnshard/tablet
    ydb/core/tx/columnshard/data_sharing/destination/transactions
)

END()
