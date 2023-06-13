LIBRARY()

SRCS(
    accessor.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/public/lib/operation_id/protos
    ydb/public/sdk/cpp/client/ydb_params
    ydb/public/sdk/cpp/client/ydb_value
    ydb/library/yql/public/issue/protos
)

END()
