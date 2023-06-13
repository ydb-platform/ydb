LIBRARY()


SRCS(
    access_service_mock.h
    datastreams_service_mock.h
    folder_service_transitional_mock.h
    folder_service_mock.h
    iam_token_service_mock.h
    service_account_service_mock.h
    user_account_service_mock.h
)

PEERDIR(
    ydb/public/api/client/yc_private/servicecontrol
    ydb/public/api/grpc/draft
    ydb/public/api/client/yc_private/resourcemanager
    ydb/public/api/client/yc_private/iam
)

END()
