LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

PEERDIR(
    contrib/libs/protobuf
    ydb/public/sdk/cpp/src/library/grpc/client
    ydb/public/sdk/cpp/src/library/yql_common/issue
)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/s3_settings.h)
GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h)

END()
