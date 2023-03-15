PROTO_LIBRARY()

SRCS(
    issue_id.proto
)

PEERDIR(
    ydb/library/yql/public/issue/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
