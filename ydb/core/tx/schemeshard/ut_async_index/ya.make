UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/base
    ydb/core/scheme
    ydb/core/testlib/default
    ydb/core/tx/schemeshard/ut_helpers
    ydb/public/lib/deprecated/kicli
)

SRCS(
    ut_async_index.cpp
)

YQL_LAST_ABI_VERSION()

END()
