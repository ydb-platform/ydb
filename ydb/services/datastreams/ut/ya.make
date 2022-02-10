UNITTEST_FOR(ydb/services/datastreams) 

OWNER( 
    g:kikimr 
    g:yql 
) 
 
FORK_SUBTESTS()

ENV(PERSQUEUE_NEW_SCHEMECACHE="true")

SIZE(MEDIUM)

TIMEOUT(600)

SRCS(
    datastreams_ut.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/grpc/client 
    library/cpp/svnversion
    ydb/core/testlib 
    ydb/services/datastreams 
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(ram:11)

END()
