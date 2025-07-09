LIBRARY()

SRCS(
    coordination.cpp
    proto_accessor.cpp
)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/impl/ydb_internal/make_request
    ydb/public/sdk/cpp/src/client/common_client
    ydb/public/sdk/cpp/src/client/common_client/impl
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/types
    ydb/public/sdk/cpp/src/client/types/status
)

END()
