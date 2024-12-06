LIBRARY()

SRCS(
    metrics_registry.cpp
    sensors_group.cpp
    service_counters.cpp
)

PEERDIR(
    library/cpp/logger/global
    library/cpp/monlib/dynamic_counters
    yql/essentials/providers/common/metrics/protos
)

END()
