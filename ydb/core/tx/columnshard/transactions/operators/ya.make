LIBRARY()

SRCS(
    GLOBAL schema.cpp
    GLOBAL backup.cpp
    GLOBAL sharing.cpp
    GLOBAL restore.cpp
    propose_tx.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/backup/import
    ydb/core/tx/columnshard/data_sharing/destination/events
    ydb/core/tx/columnshard/export/session
    ydb/core/tx/columnshard/transactions/operators/ev_write
)

END()
