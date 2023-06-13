LIBRARY()

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(metric_value_type.h)

SRCS(
    ewma.cpp
    fake.cpp
    histogram_collector_explicit.cpp
    histogram_collector_exponential.cpp
    histogram_collector_linear.cpp
    histogram_snapshot.cpp
    log_histogram_snapshot.cpp
    labels.cpp
    metric_registry.cpp
    metric_consumer.cpp
    metric_type.cpp
    metric_value.cpp
    summary_collector.cpp
    summary_snapshot.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
