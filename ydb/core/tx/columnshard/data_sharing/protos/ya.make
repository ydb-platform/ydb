PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    data.proto
    events.proto
    sessions.proto
    initiator.proto
    links.proto
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/columnshard/engines/protos
    ydb/core/tx/columnshard/common/protos
    ydb/library/actors/protos
    ydb/core/tx/columnshard/blobs_action/protos
)

END()
