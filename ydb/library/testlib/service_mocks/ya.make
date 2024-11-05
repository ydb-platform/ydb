LIBRARY()


SRCS(
    access_service_mock.h
    datastreams_service_mock.h
    folder_service_transitional_mock.h
    folder_service_mock.h
    iam_token_service_mock.h
    nebius_access_service_mock.h
    service_account_service_mock.h
    user_account_service_mock.h
    session_service_mock.h
)

PEERDIR(
    ydb/public/api/client/nc_private/accessservice
    ydb/public/api/client/yc_private/servicecontrol
    ydb/public/api/client/yc_private/accessservice
    ydb/public/api/grpc/draft
    ydb/public/api/client/yc_private/resourcemanager
    ydb/public/api/client/yc_private/iam
    ydb/public/api/client/yc_private/oauth
)

END()
