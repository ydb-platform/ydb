LIBRARY()

SRCS(
    tx_controller.cpp  # gn: into ydb/core/tx/columnshard
    locks_db.cpp  # gn: into ydb/core/tx/columnshard
)

PEERDIR(
    ydb/core/tablet_flat
    ydb/core/tx/data_events
    ydb/core/tx/columnshard/data_sharing/destination/events
    ydb/core/tx/columnshard/transactions/operators
    ydb/core/tx/columnshard/transactions/transactions
    ydb/core/tx/columnshard/transactions/locks
)

YQL_LAST_ABI_VERSION()
GENERATE_ENUM_SERIALIZATION(tx_controller.h)

END()
