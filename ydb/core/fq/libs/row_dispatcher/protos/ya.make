PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    events.proto
)

PEERDIR(
    ydb/library/actors/protos
    ydb/library/yql/dq/actors/protos
    ydb/library/yql/providers/pq/proto
    ydb/public/api/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
