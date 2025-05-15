UNITTEST_FOR(ydb/public/lib/ydb_cli/commands/tpcc)

SRCS(
    circular_queue_ut.cpp
    task_ut.cpp
    timer_queue_ut.cpp
)

PEERDIR(
    library/cpp/testing/gmock_in_unittest
    library/cpp/getopt/small
    ydb/public/lib/ydb_cli/commands/tpcc
)

END()
