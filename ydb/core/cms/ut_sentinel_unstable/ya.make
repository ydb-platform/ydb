UNITTEST_FOR(ydb/core/cms)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

TIMEOUT(600)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    cms_ut_common.cpp
    sentinel_ut_unstable.cpp
)

END()
