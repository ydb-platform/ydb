LIBRARY()

SRCS(
    ydb_dynamic_config.cpp
    ydb_scripting.cpp
    ydb_long_tx.cpp
)

PEERDIR(
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_types/operation
    ydb/public/sdk/cpp/client/ydb_value
)

END()

RECURSE_FOR_TESTS(
    ut
)
