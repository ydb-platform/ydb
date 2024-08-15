LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    endpoints.cpp
)

PEERDIR(
    library/cpp/monlib/metrics
    ydb/public/api/grpc
)

END()
