PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    task.proto
)

PEERDIR(
    ydb/core/protos
)

END()
