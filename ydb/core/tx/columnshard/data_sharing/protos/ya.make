PROTO_LIBRARY()

SRCS(
    data.proto
    events.proto
    sessions.proto
    initiator.proto
    links.proto
)

PEERDIR(
    ydb/core/tx/columnshard/common/protos
    ydb/library/actors/protos
    ydb/core/tx/columnshard/blobs_action/protos
    ydb/core/protos

)

END()
