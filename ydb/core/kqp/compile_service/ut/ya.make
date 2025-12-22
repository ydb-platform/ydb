UNITTEST_FOR(ydb/core/kqp/compile_service)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_compile_fallback_ut.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/core/kqp/compile_service
    ydb/core/kqp/common
    ydb/public/sdk/cpp/src/client/proto
    library/cpp/testing/unittest
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()

