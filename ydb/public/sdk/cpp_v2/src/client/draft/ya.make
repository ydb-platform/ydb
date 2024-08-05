LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    ydb_dynamic_config.cpp
    ydb_replication.cpp
    ydb_scripting.cpp
)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp_v2/include/ydb-cpp-sdk/client/draft/ydb_replication.h)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/library/yql/public/issue
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp_v2/src/client/table
    ydb/public/sdk/cpp_v2/src/client/types/operation
    ydb/public/sdk/cpp_v2/src/client/value
)

END()
