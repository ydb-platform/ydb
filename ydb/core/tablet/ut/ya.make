UNITTEST_FOR(ydb/core/tablet)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

TIMEOUT(600)

SPLIT_FACTOR(50)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    pipe_tracker_ut.cpp
    resource_broker_ut.cpp
    tablet_counters_ut.cpp
    tablet_counters_aggregator_ut.cpp
    tablet_metrics_ut.cpp
    tablet_pipe_ut.cpp
    tablet_pipecache_ut.cpp
    tablet_req_blockbs_ut.cpp
    tablet_resolver_ut.cpp
)

END()
