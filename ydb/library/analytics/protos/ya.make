PROTO_LIBRARY()

# gn: move into parent
PROTOC_FATAL_WARNINGS()

SUBSCRIBER(g:kikimr)

SRCS(
    data.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
