UNITTEST_FOR(ydb/core/tx/balance_coverage) 

OWNER( 
    svc 
    g:kikimr 
) 

FORK_SUBTESTS()
 
IF (SANITIZER_TYPE)
    TIMEOUT(600)
    SIZE(MEDIUM)
ELSE()
    TIMEOUT(60)
    SIZE(SMALL)
ENDIF()

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib 
)

SRCS(
    balance_coverage_builder_ut.cpp
)

END()
