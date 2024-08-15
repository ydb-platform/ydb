PROTO_LIBRARY()

SRCS(
    issue_id.proto
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/yql_common/issue/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
