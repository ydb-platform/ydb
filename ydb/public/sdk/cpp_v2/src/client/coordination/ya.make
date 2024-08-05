LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    coordination.cpp
    proto_accessor.cpp
)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp_v2/include/ydb-cpp-sdk/client/coordination/coordination.h)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_internal/make_request
    ydb/public/sdk/cpp_v2/src/client/common_client
    ydb/public/sdk/cpp_v2/src/client/common_client/impl
    ydb/public/sdk/cpp_v2/src/client/driver
    ydb/public/sdk/cpp_v2/src/client/proto
    ydb/public/sdk/cpp_v2/src/client/types
    ydb/public/sdk/cpp_v2/src/client/types/status
)

END()
