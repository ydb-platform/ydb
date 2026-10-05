LIBRARY()

SRCS(
    GLOBAL gc_counters.cpp
    GLOBAL broken_txs.cpp
    GLOBAL clean_orphaned_operations.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/normalizer/abstract
    ydb/core/tx/columnshard/blobs_action
)

END()
