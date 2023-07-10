RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    access_service.cpp
    access_service.h
    folder_service.cpp
    folder_service.h
    folder_service_transitional.cpp
    folder_service_transitional.h
    folder_service_adapter.cpp
    grpc_service_cache.h
    grpc_service_client.h
    grpc_service_settings.h
    iam_token_service.cpp
    iam_token_service.h
    mock_access_service.cpp
    mock_access_service.h
    service_account_service.cpp
    service_account_service.h
    user_account_service.cpp
    user_account_service.h
)

PEERDIR(
    ydb/library/ycloud/api
    library/cpp/actors/core
    library/cpp/digest/crc32c
    library/cpp/grpc/client
    library/cpp/json
    ydb/core/base
    ydb/library/services
    ydb/public/lib/deprecated/client
    ydb/public/lib/deprecated/kicli
)

END()
