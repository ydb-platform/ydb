IF (NOT WITH_VALGRIND)
    UNITTEST_FOR(ydb/core/tx/schemeshard) 

    OWNER(g:kikimr) 

    TIMEOUT(600) 

    SIZE(MEDIUM) 

    INCLUDE(${ARCADIA_ROOT}/ydb/tests/supp/ubsan_supp.inc)

    PEERDIR( 
        library/cpp/getopt 
        library/cpp/regex/pcre 
        library/cpp/svnversion 
        ydb/core/testlib 
        ydb/core/tx 
        ydb/core/tx/schemeshard/ut_helpers 
        ydb/library/yql/public/udf/service/exception_policy 
    ) 

    YQL_LAST_ABI_VERSION() 

    SRCS( 
        ut_upgradesubdomain.cpp 
    ) 

    END() 
ENDIF()
