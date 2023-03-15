PROTO_LIBRARY()

PEERDIR(
    ydb/public/api/protos
    ydb/library/yql/dq/actors/protos
)

SRCS(
    fq_private.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
