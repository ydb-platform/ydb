LIBRARY()

SRCS(
    access_service.h
    folder_service.h
    folder_service_transitional.h
    iam_token_service.h
    user_account_service.h
)

PEERDIR(
    ydb/public/api/client/yc_private/iam
    ydb/public/api/client/yc_private/servicecontrol
    ydb/public/api/client/yc_private/accessservice
    ydb/public/api/client/yc_private/resourcemanager
    ydb/library/actors/core
    ydb/library/grpc/client
    ydb/core/base
    ydb/core/grpc_caching
)

END()
