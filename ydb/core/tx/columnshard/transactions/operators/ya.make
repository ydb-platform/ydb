LIBRARY()

SRCS(
    GLOBAL schema.cpp  # gn: into ydb/core/tx/columnshard
    GLOBAL backup.cpp  # gn: into ydb/core/tx/columnshard
    GLOBAL sharing.cpp  # gn: into ydb/core/tx/columnshard
    GLOBAL restore.cpp  # gn: into ydb/core/tx/columnshard
    propose_tx.cpp  # gn: into ydb/core/tx/columnshard
)

PEERDIR(
    ydb/core/tx/columnshard/backup/import
    ydb/core/tx/columnshard/data_sharing/destination/events
    ydb/core/tx/columnshard/export/session
    ydb/core/tx/columnshard/transactions/operators/ev_write
)

END()
