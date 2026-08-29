UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

# Runs every op in this suite through the scheme change outbox and fails the
# test if any of them cannot resolve a target path.
ENV(YDB_SCHEME_CHANGE_CORPUS=1)

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
