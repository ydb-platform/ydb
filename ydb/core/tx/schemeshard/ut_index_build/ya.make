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
    ydb/public/sdk/cpp/src/client/table
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_index_build.cpp
    ut_vector_index_build.cpp
)

END()
