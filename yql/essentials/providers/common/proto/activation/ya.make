PROTO_LIBRARY()

SRCS(
    activation.proto
)

EXCLUDE_TAGS(GO_PROTO)

PEERDIR_FROM_POLICY(
    ALLOW
    .*
)

END()
