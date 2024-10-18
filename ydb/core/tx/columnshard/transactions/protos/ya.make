PROTO_LIBRARY()

SRCS(
    tx_event.proto
)

PEERDIR(
    ydb/core/tx/columnshard/common/protos
    ydb/core/protos
)

END()
