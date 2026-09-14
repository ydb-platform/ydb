PROTO_LIBRARY()

SRCS(
    runtime_settings.proto
)

PEERDIR(
    yql/essentials/providers/common/proto/activation
)

EXCLUDE_TAGS(GO_PROTO)

END()
