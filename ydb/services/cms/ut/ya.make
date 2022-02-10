UNITTEST_FOR(ydb/services/cms)

OWNER(g:kikimr) 
 
FORK_SUBTESTS() 
 
IF (SANITIZER_TYPE OR WITH_VALGRIND) 
    SIZE(MEDIUM) 
ENDIF() 
 
SRCS( 
    cms_ut.cpp 
) 
 
PEERDIR( 
    library/cpp/getopt
    library/cpp/grpc/client
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib
    ydb/services/cms
) 
 
YQL_LAST_ABI_VERSION()

END() 
