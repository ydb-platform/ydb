LIBRARY()

SRCS(
    GLOBAL secondary.cpp
    GLOBAL simple.cpp
    GLOBAL primary.cpp
    abstract.cpp
    sync.cpp
)

PEERDIR(
    ydb/services/metadata/abstract
    ydb/core/tx/columnshard/blobs_action/events
    ydb/core/tx/columnshard/data_sharing/destination/events
    ydb/core/tx/columnshard/transactions/locks
)

END()
