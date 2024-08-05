LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    monitoring.cpp
)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp_v2/include/ydb-cpp-sdk/client/monitoring/monitoring.h)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/client/proto
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_internal/make_request
    ydb/public/sdk/cpp_v2/src/client/common_client/impl
    ydb/public/sdk/cpp_v2/src/client/driver
)

END()

