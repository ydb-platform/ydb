LIBRARY()

SRCS(
    tx_start_to_source.cpp
    tx_data_ack_to_source.cpp
    tx_finish_ack_to_source.cpp
    tx_write_source_cursor.cpp
    tx_start_source_cursor.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/tablet
)

END()
