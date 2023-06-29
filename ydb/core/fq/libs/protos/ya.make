PROTO_LIBRARY()

PEERDIR(
    ydb/core/fq/libs/config/protos
    ydb/library/yql/dq/actors/protos
    ydb/public/api/protos
)

SRCS(
    dq_effects.proto
    fq_private.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
