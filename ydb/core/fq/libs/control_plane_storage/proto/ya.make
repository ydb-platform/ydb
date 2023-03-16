PROTO_LIBRARY()

PEERDIR(
    ydb/core/fq/libs/protos
    ydb/library/yql/dq/proto
    ydb/library/yql/providers/dq/api/protos
    ydb/public/api/protos
)

SRCS(
    yq_internal.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
