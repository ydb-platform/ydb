LIBRARY()

SRCS(
    metrics.cpp
    observation.cpp
    span.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/metrics
    ydb/public/sdk/cpp/src/client/impl/stats
)

END()
