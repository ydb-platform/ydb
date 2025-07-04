PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    metadata.proto
    profile_service.proto
    sensitive.proto
    service_account.proto
    tenant_user_account.proto
    token.proto
    token_service.proto
    token_exchange_service.proto
    user_account.proto
    validate.proto
)

END()
