PROTO_LIBRARY()

OWNER(g:yql)

SRCS(
    dq_events.proto
    dq_stats.proto
)

PEERDIR(
    library/cpp/actors/protos
    ydb/public/api/protos
    ydb/library/yql/core/issue/protos 
    ydb/library/yql/dq/proto 
    ydb/library/yql/public/issue/protos 
    ydb/library/yql/public/types 
)

EXCLUDE_TAGS(GO_PROTO)

END()
