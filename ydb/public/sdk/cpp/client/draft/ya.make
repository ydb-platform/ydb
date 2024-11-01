LIBRARY()

SRCS(
    ydb_dynamic_config.cpp
    ydb_replication.cpp
    ydb_scripting.cpp
)

GENERATE_ENUM_SERIALIZATION(ydb_replication.h)

PEERDIR(
    ydb/library/yql/public/issue
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_types/operation
    ydb/public/sdk/cpp/client/ydb_value
)

END()

RECURSE_FOR_TESTS(
    ut
)
