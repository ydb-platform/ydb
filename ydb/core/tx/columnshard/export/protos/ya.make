PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    cursor.proto
    selector.proto
    task.proto
)

PEERDIR(
    ydb/core/tx/columnshard/common/protos
    ydb/core/protos
)

END()
