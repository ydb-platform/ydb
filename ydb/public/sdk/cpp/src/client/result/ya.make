LIBRARY()

SRCS(
    out.cpp
    proto_accessor.cpp
    result.cpp
)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h)

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/types/fatal_error_handlers
    ydb/public/sdk/cpp/src/client/value
    ydb/public/sdk/cpp/src/client/proto
)

END()
