UNITTEST_FOR(ydb/core/kqp/runtime)
 
OWNER(g:kikimr) 
 
FORK_SUBTESTS() 
 
IF (SANITIZER_TYPE OR WITH_VALGRIND) 
    SIZE(MEDIUM) 
ENDIF() 
 
SRCS( 
    # kqp_spilling_file_ut.cpp
    kqp_scan_data_ut.cpp
) 
 
YQL_LAST_ABI_VERSION()

PEERDIR( 
    library/cpp/testing/unittest
    ydb/core/testlib/basics
    ydb/library/yql/public/udf/service/exception_policy
) 
 
END() 
