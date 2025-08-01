LIBRARY()

SRCS(
    accessor.cpp
)

CFLAGS(
    GLOBAL -DYDB_SDK_INTERNAL_CLIENTS
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/params
    ydb/public/sdk/cpp/src/client/value
)

END()
