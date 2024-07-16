PROTO_LIBRARY()

SRCS(
    events.proto
)

PEERDIR(
    ydb/library/actors/protos
    ydb/library/yql/providers/pq/proto
    ydb/library/yql/dq/actors/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
