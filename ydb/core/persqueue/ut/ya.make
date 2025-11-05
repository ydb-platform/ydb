UNITTEST_FOR(ydb/core/persqueue)

ADDINCL(
    ydb/public/sdk/cpp
)

FORK_SUBTESTS()

SPLIT_FACTOR(40)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/json
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils

    ydb/core/tx/schemeshard/ut_helpers
    ydb/library/dbgtrace
)

YQL_LAST_ABI_VERSION()

SRCS(
    counters_ut.cpp
    pqtablet_mock.cpp
    internals_ut.cpp
    make_config.cpp
    metering_sink_ut.cpp
    partition_chooser_ut.cpp
    pq_ut.cpp
    partition_ut.cpp
    partitiongraph_ut.cpp
    pqtablet_ut.cpp
    quota_tracker_ut.cpp
    sourceid_ut.cpp
    user_info_ut.cpp
    pqrb_describes_ut.cpp
    partition_scale_manager_graph_cmp_ut.cpp
    utils_ut.cpp
)

RESOURCE(
    ydb/core/persqueue/ut/resources/counters_datastreams.html counters_datastreams.html
    ydb/core/persqueue/ut/resources/counters_pqproxy_firstclass.html counters_pqproxy_firstclass.html
    ydb/core/persqueue/ut/resources/counters_topics.html counters_topics.html
    ydb/core/persqueue/ut/resources/counters_topics_extended.html counters_topics_extended.html

    ydb/core/persqueue/ut/resources/partition_counters/federation/after_write.html federation_after_write.html
    ydb/core/persqueue/ut/resources/partition_counters/federation/after_read.html federation_after_read.html
    ydb/core/persqueue/ut/resources/partition_counters/federation/turned_off.html federation_turned_off.html
    ydb/core/persqueue/ut/resources/partition_counters/first_class_citizen/after_write.html first_class_citizen_after_write.html
    ydb/core/persqueue/ut/resources/partition_counters/first_class_citizen/after_read.html first_class_citizen_after_read.html
    ydb/core/persqueue/ut/resources/partition_counters/first_class_citizen/turned_off.html first_class_citizen_turned_off.html

    ydb/core/persqueue/ut/resources/counters_pqproxy.html counters_pqproxy.html

    ydb/core/persqueue/ut/resources/counters_labeled.json counters_labeled.json
)

END()
