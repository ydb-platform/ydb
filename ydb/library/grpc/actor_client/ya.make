LIBRARY()

SRCS(
    grpc_service_cache.h
    grpc_service_client.h
    grpc_service_settings.h
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/util
    library/cpp/digest/crc32c
    ydb/library/grpc/client
    ydb/library/services




    #ydb/library/ycloud/api
    #ydb/library/actors/core
    #library/cpp/digest/crc32c
    #ydb/library/grpc/client
    #library/cpp/json
    #ydb/core/base
    #ydb/library/services
    #ydb/public/lib/deprecated/client
    #ydb/public/lib/deprecated/kicli
)

END()
