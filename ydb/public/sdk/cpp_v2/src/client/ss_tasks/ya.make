LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    task.cpp
    out.cpp
)

GENERATE_ENUM_SERIALIZATION(task.h)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/sdk/cpp_v2/src/client/common_client/impl
    ydb/public/sdk/cpp_v2/src/client/driver
    ydb/public/sdk/cpp_v2/src/client/proto
    ydb/public/sdk/cpp_v2/src/client/types/operation
)

END()
