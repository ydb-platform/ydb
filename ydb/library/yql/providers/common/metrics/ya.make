LIBRARY()

SRCS(
    metrics_registry.cpp
    sensors_group.cpp
    service_counters.cpp
)

PEERDIR(
    library/cpp/logger/global
    library/cpp/monlib/dynamic_counters
    ydb/library/yql/providers/common/metrics/protos
)

END()
