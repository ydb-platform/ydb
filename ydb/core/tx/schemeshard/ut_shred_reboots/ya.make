UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(10)

TIMEOUT(900)

SIZE(LARGE)
TAG(ya:fat)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    ydb/core/cms
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/wrappers/ut_helpers
)

SRCS(
    ut_shred_reboots.cpp
)

YQL_LAST_ABI_VERSION()

END()
