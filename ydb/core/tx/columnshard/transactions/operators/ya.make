LIBRARY()

SRCS(
    GLOBAL schema.cpp
    GLOBAL long_tx_write.cpp
    GLOBAL backup.cpp
    GLOBAL sharing.cpp
    propose_tx.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/data_sharing/destination/events
    ydb/core/tx/columnshard/transactions/operators/ev_write
    ydb/core/tx/columnshard/export/session
)

END()
