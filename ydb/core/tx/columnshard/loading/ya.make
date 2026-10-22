LIBRARY()

SRCS(
    stages.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/tx_reader
    ydb/core/tx/columnshard/tracing
    ydb/services/metadata/abstract
    ydb/core/tx/columnshard/blobs_action/events
    ydb/core/tx/columnshard/data_sharing
)

END()
