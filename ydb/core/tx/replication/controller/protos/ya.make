PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

PEERDIR(
    ydb/core/protos
    ydb/core/scheme/protos
    ydb/public/api/protos
)

SRCS(
    cdc_target.proto
    schema_barrier.proto
)

END()
