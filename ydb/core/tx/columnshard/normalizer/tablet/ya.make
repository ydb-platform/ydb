LIBRARY()

SRCS(
    GLOBAL gc_counters.cpp
    GLOBAL broken_txs.cpp
    GLOBAL clean_orphaned_operations.cpp
    GLOBAL clean_stale_backup_transactions.cpp
)

PEERDIR(
    library/cpp/json/writer
    library/cpp/string_utils/base64
    ydb/core/tx/columnshard/normalizer/abstract
    ydb/core/tx/columnshard/blobs_action
)

END()
