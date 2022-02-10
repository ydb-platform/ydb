LIBRARY()

OWNER(
    dcherednik
    g:kikimr
)

SRCS(
    coordination.cpp
)

GENERATE_ENUM_SERIALIZATION(coordination.h)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/sdk/cpp/client/impl/ydb_internal/make_request
    ydb/public/sdk/cpp/client/ydb_common_client
    ydb/public/sdk/cpp/client/ydb_common_client/impl
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_types
    ydb/public/sdk/cpp/client/ydb_types/status
)

END()

RECURSE_FOR_TESTS(
    ut
)
