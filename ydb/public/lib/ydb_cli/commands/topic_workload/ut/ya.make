UNITTEST_FOR(ydb/public/lib/ydb_cli/commands/topic_workload)

SRCS(
    topic_workload_params_ut.cpp
    topic_workload_writer_producer_ut.cpp
)

PEERDIR(
    library/cpp/testing/gmock_in_unittest
    library/cpp/regex/pcre
    library/cpp/getopt/small
    ydb/public/lib/ydb_cli/commands/topic_workload
)

END()
