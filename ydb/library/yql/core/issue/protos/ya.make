PROTO_LIBRARY()

SRCS(
    issue_id.proto
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/library/yql_common/issue/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
