UNITTEST_FOR(ydb/core/cms)

OWNER(
    ienkovich
    g:kikimr
)

SPLIT_FACTOR(30) 

FORK_SUBTESTS() 

SIZE(MEDIUM) 

TIMEOUT(600) 

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

SRCS(
    cluster_info_ut.cpp
    cms_ut.cpp
    cms_tenants_ut.cpp
    cms_ut_common.cpp
    cms_ut_common.h
    downtime_ut.cpp
    ut_helpers.cpp
)

END()
