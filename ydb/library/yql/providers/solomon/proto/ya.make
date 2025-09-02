PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    dq_solomon_shard.proto
    metrics_queue.proto
)

PEERDIR(
    ydb/library/yql/dq/actors/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
