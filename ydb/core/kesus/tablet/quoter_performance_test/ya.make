PROGRAM()

OWNER( 
    galaxycrab 
    g:kikimr 
) 

SRCDIR(ydb/core/kesus/tablet) 

PEERDIR(
    library/cpp/getopt
    library/cpp/testing/unittest
    ADDINCL ydb/core/kesus/tablet 
    ydb/core/testlib 
)

YQL_LAST_ABI_VERSION() 

SRCS(
    main.cpp
    ut_helpers.cpp
)

END()
