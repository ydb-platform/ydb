LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    operation.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/sdk/cpp_v2/src/library/operation_id
    ydb/public/sdk/cpp_v2/src/client/query
    ydb/public/sdk/cpp_v2/src/client/common_client/impl
    ydb/public/sdk/cpp_v2/src/client/driver
    ydb/public/sdk/cpp_v2/src/client/export
    ydb/public/sdk/cpp_v2/src/client/import
    ydb/public/sdk/cpp_v2/src/client/ss_tasks
    ydb/public/sdk/cpp_v2/src/client/types/operation
)

END()
