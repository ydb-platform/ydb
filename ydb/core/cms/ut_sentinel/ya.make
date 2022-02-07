UNITTEST_FOR(ydb/core/cms)

OWNER(
    ilnaz
    g:kikimr
)

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

SRCS(
    cms_ut_common.cpp
    cms_ut_common.h
    sentinel_ut.cpp
)

END()
