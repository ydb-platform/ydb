PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    counters_shard.proto
)

PEERDIR(
    ydb/core/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
