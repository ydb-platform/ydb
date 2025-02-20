LIBRARY()

SRCS(
    access_service.h
    folder_service.h
    folder_service_transitional.h
    iam_token_service.h
    user_account_service.h
)

PEERDIR(
    ydb/public/api/client/yc_private/iam/v1
    ydb/public/api/client/yc_private/servicecontrol/v1
    ydb/public/api/client/yc_private/accessservice/v2
    ydb/public/api/client/yc_private/resourcemanager/v1
    ydb/library/actors/core
    ydb/public/sdk/cpp/src/library/grpc/client
    ydb/core/base
    ydb/core/grpc_caching
)

END()
