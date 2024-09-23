LIBRARY()

SRCS(
    task.cpp
    out.cpp
)

GENERATE_ENUM_SERIALIZATION(task.h)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/ydb_common_client/impl
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/ydb_types/operation
)

END()
