UNITTEST_FOR(ydb/services/datastreams)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

SRCS(
    datastreams_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/library/grpc/client
    library/cpp/svnversion
    ydb/core/testlib/default
    ydb/services/datastreams
    ydb/public/sdk/cpp/client/ydb_topic
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(ram:11)

END()
