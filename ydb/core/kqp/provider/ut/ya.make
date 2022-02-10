UNITTEST_FOR(ydb/core/kqp/provider)

OWNER(spuchin)

SRCS(
    yql_kikimr_gateway_ut.cpp
)

PEERDIR(
    ydb/core/client/minikql_result_lib
    ydb/core/kqp/ut/common
)

YQL_LAST_ABI_VERSION()

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE) 
    TAG(ya:fat) 
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

END()
