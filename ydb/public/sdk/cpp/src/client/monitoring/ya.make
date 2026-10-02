LIBRARY()

SRCS(
    monitoring.cpp
)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/monitoring/monitoring.h)

PEERDIR(
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/impl/internal/make_request
    ydb/public/sdk/cpp/src/client/common_client/impl
    ydb/public/sdk/cpp/src/client/driver
)

END()

