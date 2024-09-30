UNITTEST_FOR(ydb/services/datastreams)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

SRCS(
    datastreams_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
    ydb/core/tx/schemeshard/ut_helpers
    ydb/library/grpc/client
    ydb/public/sdk/cpp/client/ydb_topic
    ydb/services/datastreams

    ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:11)
ENDIF()

END()
