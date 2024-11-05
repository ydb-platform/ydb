PROTO_LIBRARY()

SRCS(
    counters_shard.proto
)

PEERDIR(
    ydb/core/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
