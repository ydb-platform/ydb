PROTO_LIBRARY()

SRCS(
    issue_id.proto
)

PEERDIR(
    yql/essentials/public/issue/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
