PROTO_LIBRARY(yt-client-hedging-proto)

SRCS(
    proto/config.proto
)

PEERDIR(
    yt/yt_proto/yt/core
    yt/yt_proto/yt/client/cache
)

EXCLUDE_TAGS(GO_PROTO)

END()
