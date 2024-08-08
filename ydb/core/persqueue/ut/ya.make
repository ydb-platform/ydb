UNITTEST_FOR(ydb/core/persqueue)

FORK_SUBTESTS()

SPLIT_FACTOR(40)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    TIMEOUT(3000)
ELSE()
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
    TIMEOUT(600)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
    ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils
    ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils

    ydb/core/tx/schemeshard/ut_helpers
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
    type_codecs_ut.cpp
    user_info_ut.cpp
    pqrb_describes_ut.cpp
    microseconds_sliding_window_ut.cpp
    fetch_request_ut.cpp
    utils_ut.cpp
)

RESOURCE(
    ydb/core/persqueue/ut/resources/counters_datastreams.html counters_datastreams.html
    ydb/core/persqueue/ut/resources/counters_pqproxy_firstclass.html counters_pqproxy_firstclass.html
    ydb/core/persqueue/ut/resources/counters_topics.html counters_topics.html

    ydb/core/persqueue/ut/resources/counters_pqproxy.html counters_pqproxy.html

    ydb/core/persqueue/ut/resources/counters_labeled.json counters_labeled.json
)

END()
