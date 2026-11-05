PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    schema_barrier.proto
    secret_copy.proto
)

PEERDIR(
    ydb/public/api/protos/annotations
)

END()
