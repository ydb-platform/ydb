LIBRARY()

SRCS(
    GLOBAL secondary.cpp  # gn: into ydb/core/tx/columnshard
    GLOBAL simple.cpp  # gn: into ydb/core/tx/columnshard
    GLOBAL primary.cpp  # gn: into ydb/core/tx/columnshard
    abstract.cpp  # gn: into ydb/core/tx/columnshard
    sync.cpp  # gn: into ydb/core/tx/columnshard
)

PEERDIR(
    ydb/services/metadata/abstract
    ydb/core/tx/columnshard/blobs_action/events
    ydb/core/tx/columnshard/data_sharing/destination/events
    ydb/core/tx/columnshard/transactions/locks
    ydb/core/tx/columnshard/tracing
)

END()
