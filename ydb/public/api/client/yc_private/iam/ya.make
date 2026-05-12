PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

PEERDIR(
    ydb/public/api/client/yc_private/accessservice
    ydb/public/api/client/yc_private/operation
)

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    iam_token_service_subject.proto
    iam_token_service.proto
    iam_token.proto
    oauth_request.proto
    reference.proto
    service_account_service.proto
    service_account.proto
    user_account_service.proto
    user_account.proto
    yandex_passport_cookie.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()

