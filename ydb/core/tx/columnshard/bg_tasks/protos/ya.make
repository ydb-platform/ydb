PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    data.proto
)

PEERDIR(
    ydb/core/tx/columnshard/common/protos
    ydb/services/bg_tasks/protos
)

END()
