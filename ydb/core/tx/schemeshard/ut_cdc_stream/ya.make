UNITTEST_FOR(ydb/core/tx/schemeshard)

OWNER(
    ilnaz
    g:kikimr
)

FORK_SUBTESTS()

SPLIT_FACTOR(2)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/testlib
    ydb/core/tx/schemeshard/ut_helpers
)

SRCS(
    ut_cdc_stream.cpp
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(ram:11) 
 
END()
