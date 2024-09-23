LIBRARY()

SRCS(
    operation.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/lib/operation_id
    ydb/public/sdk/cpp/client/ydb_query
    ydb/public/sdk/cpp/client/ydb_common_client/impl
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_export
    ydb/public/sdk/cpp/client/ydb_import
    ydb/public/sdk/cpp/client/ydb_ss_tasks
    ydb/public/sdk/cpp/client/ydb_types/operation
)

END()
