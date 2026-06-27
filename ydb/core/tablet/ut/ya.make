UNITTEST_FOR(ydb/core/tablet)

FORK_SUBTESTS()

SIZE(MEDIUM)

SPLIT_FACTOR(50)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    bootstrapper_ut.cpp
    pipe_tracker_ut.cpp
    resource_broker_ut.cpp
    tablet_counters_ut.cpp
    tablet_counters_aggregator_ut.cpp
    tablet_metrics_ut.cpp
    tablet_pipe_ut.cpp
    tablet_pipecache_ut.cpp
    tablet_req_blockbs_ut.cpp
    tablet_resolver_ut.cpp
    tablet_state_ut.cpp
    detailed_metrics/node_database_metrics_aggregator_ut.cpp
    detailed_metrics/ut_helpers.cpp
    detailed_metrics/ut_helpers.h
    detailed_metrics/ut_sensors_json_builder.h
    detailed_metrics/ydb_metrics_aggregator_ut.cpp
    detailed_metrics/ydb_metrics_ut.cpp
)

END()
