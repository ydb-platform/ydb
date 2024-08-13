UNITTEST_FOR(ydb/core/persqueue)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    TIMEOUT(3000)
ELSE()
    SIZE(MEDIUM)
    TIMEOUT(60)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
    ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils
    ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils

    ydb/core/tx/schemeshard/ut_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    autoscaling_ut.cpp
    balancing_ut.cpp
    mirrorer_ut.cpp
)

END()
