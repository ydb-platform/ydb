LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

PEERDIR(
    contrib/libs/protobuf
    ydb/public/sdk/cpp_v2/src/library/grpc/client
    ydb/public/sdk/cpp_v2/src/library/yql/public/issue
)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp_v2/include/ydb-cpp-sdk/client/types/s3_settings.h)
GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp_v2/include/ydb-cpp-sdk/client/types/status_codes.h)

END()
