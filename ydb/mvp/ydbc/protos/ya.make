PROTO_LIBRARY()

SRCS(
    ydbc.proto
)

PEERDIR(
    ydb/public/api/client/yc_private/ydb
)

EXCLUDE_TAGS(GO_PROTO)

END()
