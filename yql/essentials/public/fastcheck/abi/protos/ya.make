PROTO_LIBRARY()

SRCS(
    fastcheck.proto
)

PEERDIR(
    yql/essentials/public/issue/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
