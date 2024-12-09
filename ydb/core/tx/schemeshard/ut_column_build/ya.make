UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/metering
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/schemeshard/ut_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_column_build.cpp
)

END()
