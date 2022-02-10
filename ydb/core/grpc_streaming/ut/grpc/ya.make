PROTO_LIBRARY()

GRPC()

OWNER(g:kikimr)

SRCS(
    streaming_service.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
