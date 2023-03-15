UNITTEST_FOR(ydb/core/cms)

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    cms_ut_common.cpp
    cms_ut_common.h
    sentinel_ut.cpp
)

END()
