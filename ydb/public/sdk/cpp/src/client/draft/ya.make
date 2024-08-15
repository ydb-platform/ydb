LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    ydb_dynamic_config.cpp
    ydb_replication.cpp
    ydb_scripting.cpp
)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h)

PEERDIR(
    ydb/public/sdk/cpp/src/library/yql_common/issue
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/types/operation
    ydb/public/sdk/cpp/src/client/value
)

END()
