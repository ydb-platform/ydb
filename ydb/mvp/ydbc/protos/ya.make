PROTO_LIBRARY()

OWNER(xenoxeno g:kikimr)

SRCS(
    ydbc.proto
)

PEERDIR(
    ydb/public/api/client/yc_private/ydb
)

EXCLUDE_TAGS(GO_PROTO)

END()
