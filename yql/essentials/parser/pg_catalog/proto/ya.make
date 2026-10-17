PROTO_LIBRARY()

# gn: move into parent

SRCS(
    pg_catalog.proto
)

PEERDIR(
    yql/essentials/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
