LIBRARY()

SRCS(
    tx_start_to_source.cpp  # gn: into ydb/core/tx/columnshard
    tx_data_ack_to_source.cpp  # gn: into ydb/core/tx/columnshard
    tx_finish_ack_to_source.cpp  # gn: into ydb/core/tx/columnshard
    tx_write_source_cursor.cpp  # gn: into ydb/core/tx/columnshard
    tx_start_source_cursor.cpp  # gn: into ydb/core/tx/columnshard
)

PEERDIR(
    ydb/core/tx/columnshard/tablet
)

END()
