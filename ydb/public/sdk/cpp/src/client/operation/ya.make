LIBRARY()

SRCS(
    operation.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/library/operation_id
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/common_client/impl
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/export
    ydb/public/sdk/cpp/src/client/import
    ydb/public/sdk/cpp/src/client/ss_tasks
    ydb/public/sdk/cpp/src/client/types/operation
)

END()
