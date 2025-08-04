PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()

SRCS(
    access.proto
    access_service.proto
    profile_service.proto
    service_account.proto
    tenant_user_account.proto
    token.proto
    token_exchange_service.proto
    token_service.proto
    user_account.proto
)

PEERDIR(
    ydb/public/api/client/nc_private
    ydb/public/api/client/nc_private/audit
    ydb/public/api/client/nc_private/audit/v1/common
    ydb/public/api/client/nc_private/common/v1
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()
