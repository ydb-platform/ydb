LIBRARY()

SRCS(
    tx_controller.cpp
    locks_db.cpp
)

PEERDIR(
    ydb/core/tablet_flat
    ydb/core/tx/data_events
    ydb/core/tx/columnshard/data_sharing/destination/events
    ydb/core/tx/columnshard/transactions/operators
    ydb/core/tx/columnshard/transactions/transactions
)

YQL_LAST_ABI_VERSION()
GENERATE_ENUM_SERIALIZATION(tx_controller.h)

END()
