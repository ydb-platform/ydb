PROTO_LIBRARY()

SRCS(
    sql_inspect.proto
)

PEERDIR(
    yql/essentials/public/issue/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
