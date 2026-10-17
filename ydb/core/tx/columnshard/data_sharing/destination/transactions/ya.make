LIBRARY()

SRCS(
    tx_start_from_initiator.cpp  # gn: into ydb/core/tx/columnshard
    tx_data_from_source.cpp  # gn: into ydb/core/tx/columnshard
    tx_finish_from_source.cpp  # gn: into ydb/core/tx/columnshard
    tx_finish_ack_from_initiator.cpp  # gn: into ydb/core/tx/columnshard
)

PEERDIR(
    ydb/core/tx/tiering
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/core/tx/columnshard/tablet
)

END()
