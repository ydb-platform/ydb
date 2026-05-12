LIBRARY()

SRCS(
    metrics.cpp
    observation.cpp
    operation_name.cpp
    span.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/metrics
    ydb/public/sdk/cpp/src/client/trace
    ydb/public/sdk/cpp/src/client/impl/stats
    ydb/public/sdk/cpp/src/client/impl/observability/error_category
    ydb/public/sdk/cpp/src/client/impl/internal/db_driver_state
)

END()
