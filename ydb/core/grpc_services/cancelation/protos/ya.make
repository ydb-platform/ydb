PROTO_LIBRARY()

SRCS(
    event.proto
)

PEERDIR(
    library/cpp/actors/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()

