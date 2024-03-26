LIBRARY()

SRCS(
    tx_controller.cpp
    locks_db.cpp
    propose_transaction_base.cpp
)

PEERDIR(
    ydb/core/tablet_flat
    ydb/core/tx/data_events
    ydb/core/tx/columnshard/data_sharing/destination/events
)

YQL_LAST_ABI_VERSION()

END()
