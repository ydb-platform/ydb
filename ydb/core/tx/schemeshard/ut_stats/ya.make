UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(10)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    ydb/core/cms
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/datashard/ut_common
    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/wrappers/ut_helpers
)

SRCS(
    ut_stats.cpp
)

YQL_LAST_ABI_VERSION()

END()
