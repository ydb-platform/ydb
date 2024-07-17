PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    token_service.proto
    token_exchange_service.proto
)

END()
