UNITTEST_FOR(library/cpp/monlib/metrics)

SRCS(
    ewma_ut.cpp
    fake_ut.cpp
    histogram_collector_ut.cpp
    histogram_snapshot_ut.cpp
    labels_ut.cpp
    log_histogram_collector_ut.cpp
    metric_registry_ut.cpp
    metric_sub_registry_ut.cpp
    metric_value_ut.cpp
    summary_collector_ut.cpp
    timer_ut.cpp
)

RESOURCE(
    histograms.json /histograms.json
)

PEERDIR(
    library/cpp/resource
    library/cpp/monlib/encode/protobuf
    library/cpp/monlib/encode/json
    library/cpp/threading/future
)

END()
