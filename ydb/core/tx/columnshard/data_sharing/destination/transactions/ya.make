LIBRARY()

SRCS(
    tx_start_from_initiator.cpp
    tx_data_from_source.cpp
    tx_finish_from_source.cpp
    tx_finish_ack_from_initiator.cpp
)

PEERDIR(
    ydb/core/tx/tiering
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/core/tx/columnshard/data_sharing/common/transactions
)

END()
