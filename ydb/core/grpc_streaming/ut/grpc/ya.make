PROTO_LIBRARY()

GRPC()

SRCS(
    streaming_service.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
