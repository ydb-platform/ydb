LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    out.cpp
    proto_accessor.cpp
    table.cpp
)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp_v2/include/ydb-cpp-sdk/client/table/table_enum.h)

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_internal/make_request
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_internal/kqp_session_common
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_internal/retry
    ydb/public/sdk/cpp_v2/src/client/driver
    ydb/public/sdk/cpp_v2/src/client/params
    ydb/public/sdk/cpp_v2/src/client/proto
    ydb/public/sdk/cpp_v2/src/client/result
    ydb/public/sdk/cpp_v2/src/client/scheme
    ydb/public/sdk/cpp_v2/src/client/table/impl
    ydb/public/sdk/cpp_v2/src/client/table/query_stats
    ydb/public/sdk/cpp_v2/src/client/types/operation
    ydb/public/sdk/cpp_v2/src/client/value
)

END()
