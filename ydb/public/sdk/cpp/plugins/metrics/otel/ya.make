LIBRARY()

SRCS(
    metrics.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/metrics
    ydb/public/sdk/cpp/src/client/resources
    contrib/libs/opentelemetry-cpp
    contrib/libs/opentelemetry-cpp/api
)

END()
