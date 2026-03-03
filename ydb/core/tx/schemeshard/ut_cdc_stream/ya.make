UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(2)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:1)
ENDIF()

PEERDIR(
    ydb/core/testlib/default
    ydb/core/tx/schemeshard/ut_helpers
    library/cpp/json
)

SRCS(
    ut_cdc_stream.cpp
)

YQL_LAST_ABI_VERSION()

END()
