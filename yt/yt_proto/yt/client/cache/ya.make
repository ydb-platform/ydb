PROTO_LIBRARY(yt-client-cache-proto)

PROTO_NAMESPACE(yt)
PY_NAMESPACE(yt_proto.yt.client.cache)

SRCS(
    proto/config.proto
)

PEERDIR(yt/yt_proto/yt/core)

EXCLUDE_TAGS(GO_PROTO)

END()
