UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/yql_testlib
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_serverless.cpp
)

END()
