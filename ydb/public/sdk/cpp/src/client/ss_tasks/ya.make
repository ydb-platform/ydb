LIBRARY()

SRCS(
    task.cpp
    out.cpp
)

GENERATE_ENUM_SERIALIZATION(task.h)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/common_client/impl
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/types/operation
)

END()
