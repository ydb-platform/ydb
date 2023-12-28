PROTO_LIBRARY()

OWNER(
    xenoxeno
    g:kikimr
)

SRCS(
    counters_shard.proto
)

PEERDIR(
    ydb/core/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
