PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    token.proto
    token_service.proto
    token_exchange_service.proto
    profile_service.proto
    sensitive.proto
    service_account.proto
    tenant_user_account.proto
    user_account.proto
    validate.proto
)

END()
