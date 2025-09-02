PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    dq_events.proto
    dq_stats.proto
    dq_status_codes.proto
)

PEERDIR(
    ydb/library/actors/protos
    ydb/public/api/protos
    yql/essentials/core/issue/protos
    ydb/library/yql/dq/proto
    yql/essentials/public/issue/protos
    yql/essentials/public/types
)

EXCLUDE_TAGS(GO_PROTO)

END()
