LIBRARY()

SRCS(
    endpoints.cpp
)

PEERDIR(
    library/cpp/monlib/metrics
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/impl/internal/logger
)

END()
