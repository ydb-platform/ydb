PROTO_LIBRARY()

SRCS(
    access_provider.proto
)

PEERDIR(
    yt/yql/providers/yt/proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
