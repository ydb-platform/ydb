LIBRARY()

SRCS(
    counters_helper.cpp
    critical_events.cpp
    executor_counters.cpp
    histogram.cpp
    histogram_types.cpp
    logging.cpp
    monitoring.cpp
    postpone_time_predictor.cpp
    request_counters.cpp
    weighted_percentile.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/storage/core/libs/common
    ydb/core/nbs/cloud/storage/core/protos

    library/cpp/deprecated/atomic
    library/cpp/histogram/hdr
    library/cpp/json/writer
    library/cpp/logger
    library/cpp/lwtrace
    library/cpp/lwtrace/mon
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/service
    library/cpp/monlib/service/pages
    library/cpp/monlib/service/pages/tablesorter
    library/cpp/unified_agent_client
    util
    ydb/core/protos/nbs
    ydb/library/actors/core
    ydb/library/actors/prof
    ydb/library/services
)

END()

RECURSE_FOR_TESTS(ut)
