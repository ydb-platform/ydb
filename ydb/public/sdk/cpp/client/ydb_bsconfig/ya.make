LIBRARY()

SRCS(
    ydb_storage_config.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_types/operation
    ydb/public/sdk/cpp/client/ydb_value
)

END()
