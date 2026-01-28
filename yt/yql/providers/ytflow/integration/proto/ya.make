PROTO_LIBRARY()

PEERDIR(
    yql/essentials/providers/common/proto
)

SRCS(
    yt.proto
    solomon.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
