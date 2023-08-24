PROTO_LIBRARY(yt-client-cache-proto)

SRCS(
    proto/config.proto
)

PEERDIR(yt/yt_proto/yt/core)

EXCLUDE_TAGS(GO_PROTO)

END()
