PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

PEERDIR(
    ydb/core/protos
    ydb/core/scheme/protos
)

SRCS(
    cdc_target.proto
    schema_barrier.proto
)

END()
