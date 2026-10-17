LIBRARY()

SRCS(
    stages.cpp  # gn: into ydb/core/tx/columnshard
)

PEERDIR(
    ydb/core/tx/columnshard/tx_reader
    ydb/services/metadata/abstract
    ydb/core/tx/columnshard/blobs_action/events
    ydb/core/tx/columnshard/data_sharing
)

END()
